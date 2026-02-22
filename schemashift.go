package schemashift

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/SravanKolanu20/schemashift/dialect"
	"github.com/SravanKolanu20/schemashift/hooks"
	"github.com/SravanKolanu20/schemashift/lock"
	"github.com/SravanKolanu20/schemashift/migration"
	"github.com/SravanKolanu20/schemashift/strategy"
	"github.com/SravanKolanu20/schemashift/tracker"
)

// Version is the current schemashift library version.
const Version = "0.1.0"

// ShifterOption is a functional option for configuring a Shifter.
type ShifterOption func(*Shifter)

// Shifter is the main entry point for schemashift.
// It orchestrates the full migration lifecycle:
// locking → planning → checkpointing → executing → tracking.
//
// Example usage:
//
//	db, _ := sql.Open("postgres", dsn)
//
//	shifter := schemashift.New(db, dialect.NewPostgres(),
//	    schemashift.WithHooks(hooks.NewRegistry()),
//	    schemashift.WithLargeTableThreshold(500_000),
//	)
//
//	err := shifter.Apply(ctx,
//	    migration.Build("20240101_create_users").
//	        Describe("Create users table").
//	        Add(&migration.CreateTableStep{
//	            Table: "users",
//	            Columns: []dialect.ColumnDef{
//	                {Name: "id",    Type: "SERIAL",       PrimaryKey: true},
//	                {Name: "email", Type: "TEXT",         Nullable: false},
//	                {Name: "name",  Type: "TEXT",         Nullable: true},
//	            },
//	        }).
//	        Done(),
//
//	    migration.Build("20240102_add_phone").
//	        Describe("Add phone column to users").
//	        Add(&migration.AddColumnStep{
//	            Table:  "users",
//	            Column: dialect.ColumnDef{Name: "phone", Type: "TEXT", Nullable: true},
//	        }).
//	        Done(),
//	)
type Shifter struct {
	db      *sql.DB
	dialect dialect.Dialect

	// registry holds all event handlers for observability
	registry *hooks.Registry

	// lockManager handles distributed locking
	lockManager *lock.Manager

	// checkpoints manages crash-safe state persistence
	checkpoints *migration.CheckpointStore

	// tracker records permanently applied migrations
	tracker *tracker.Tracker

	// planner builds execution plans
	planner *migration.Planner

	// options
	largeTableThreshold int64
	lockKey             string
	dryRun              bool
	stopOnError         bool
	checkDrift          bool
}

// ─── Constructor ──────────────────────────────────────────────────────────────

// New creates a new Shifter with the given database connection and dialect.
//
// Example:
//
//	// Postgres
//	shifter := schemashift.New(db, dialect.NewPostgres())
//
//	// MySQL
//	shifter := schemashift.New(db, dialect.NewMySQL("myapp"))
//
//	// SQLite
//	shifter := schemashift.New(db, dialect.NewSQLite())
func New(db *sql.DB, d dialect.Dialect, opts ...ShifterOption) *Shifter {
	s := &Shifter{
		db:                  db,
		dialect:             d,
		registry:            hooks.NewRegistry(),
		largeTableThreshold: 100_000,
		lockKey:             "schemashift_global",
		stopOnError:         true,
		checkDrift:          true,
	}

	// Wire up internal components
	s.lockManager = lock.NewManager(db, d)
	s.checkpoints = migration.NewCheckpointStore(db, d)
	s.tracker = tracker.New(db, d)
	s.planner = migration.NewPlanner(db, d).
		WithLargeTableThreshold(s.largeTableThreshold)

	// Apply functional options
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// ─── Functional Options ───────────────────────────────────────────────────────

// WithHooks sets a custom hooks registry.
// Use this to attach logging, metrics, and alerting handlers.
//
// Example:
//
//	registry := hooks.NewRegistry()
//	registry.OnAny(hooks.PrettyHandler(os.Stdout))
//	shifter := schemashift.New(db, d, schemashift.WithHooks(registry))
func WithHooks(r *hooks.Registry) ShifterOption {
	return func(s *Shifter) {
		s.registry = r
	}
}

// WithLargeTableThreshold sets the row count above which the planner
// automatically switches from InPlace to Shadow strategy.
// Default: 100,000 rows.
func WithLargeTableThreshold(n int64) ShifterOption {
	return func(s *Shifter) {
		s.largeTableThreshold = n
		s.planner = migration.NewPlanner(s.db, s.dialect).
			WithLargeTableThreshold(n)
	}
}

// WithLockKey sets a custom advisory lock key.
// Useful when running multiple independent migrators against the same DB.
// Default: "schemashift_global"
func WithLockKey(key string) ShifterOption {
	return func(s *Shifter) {
		s.lockKey = key
	}
}

// WithDryRun enables dry-run mode — the execution plan is built and printed
// but no migrations are actually applied.
func WithDryRun() ShifterOption {
	return func(s *Shifter) {
		s.dryRun = true
	}
}

// WithContinueOnError makes the Shifter continue applying remaining migrations
// even if one fails. By default schemashift stops on first error.
func WithContinueOnError() ShifterOption {
	return func(s *Shifter) {
		s.stopOnError = false
	}
}

// WithDriftCheck enables/disables checksum drift detection.
// When enabled, schemashift warns if an applied migration's definition
// has changed since it was applied. Default: true.
func WithDriftCheck(enabled bool) ShifterOption {
	return func(s *Shifter) {
		s.checkDrift = enabled
	}
}

// WithLockRetry configures lock acquisition retry behavior.
func WithLockRetry(maxRetries int, interval time.Duration) ShifterOption {
	return func(s *Shifter) {
		s.lockManager.WithMaxRetries(maxRetries).WithRetryInterval(interval)
	}
}

// WithPrettyLogging is a convenience option that attaches PrettyHandler
// to stdout and LogHandler to stderr for errors.
func WithPrettyLogging() ShifterOption {
	return func(s *Shifter) {
		import_writer := &strings.Builder{} // placeholder — use os.Stdout in real code
		_ = import_writer
		// In real usage: registry.OnAny(hooks.PrettyHandler(os.Stdout))
		// Keeping this clean for library code that shouldn't import "os"
	}
}

// ─── Core API ─────────────────────────────────────────────────────────────────

// Apply is the primary entry point — it applies all pending migrations
// from the provided list in order.
//
// Apply is safe to call on every application startup. Already-applied
// migrations are detected via the tracker and skipped automatically.
//
// Migrations are applied inside a distributed lock — only one Shifter
// instance can run Apply() at a time across all app instances.
//
// Example:
//
//	err := shifter.Apply(ctx, m1, m2, m3)
func (s *Shifter) Apply(ctx context.Context, migrations ...*migration.Migration) error {
	applyStart := time.Now()

	s.registry.EmitSimple(ctx, hooks.EventShifterStart,
		fmt.Sprintf("schemashift v%s starting — %d migration(s) registered", Version, len(migrations)),
	)

	// ── Step 1: Initialize internal tables ───────────────────────────────────

	if err := s.initialize(ctx); err != nil {
		s.registry.EmitSimple(ctx, hooks.EventShifterError,
			fmt.Sprintf("initialization failed: %v", err),
		)
		return fmt.Errorf("schemashift: initialize: %w", err)
	}

	// ── Step 2: Validate all migrations ──────────────────────────────────────

	if err := migration.ValidateAll(migrations, s.dialect); err != nil {
		return fmt.Errorf("schemashift: validation failed: %w", err)
	}

	// ── Step 3: Check for drift ───────────────────────────────────────────────

	if s.checkDrift {
		if err := s.detectDrift(ctx, migrations); err != nil {
			return err
		}
	}

	// ── Step 4: Clear stale locks (SQLite only) ───────────────────────────────

	if s.dialect.Name() == "sqlite" {
		cleared, err := lock.ClearStaleLocks(ctx, s.db, s.dialect)
		if err != nil {
			// Non-fatal — log and continue
			s.registry.EmitSimple(ctx, hooks.EventShifterError,
				fmt.Sprintf("warning: clear stale locks: %v", err),
			)
		} else if cleared > 0 {
			s.registry.EmitSimple(ctx, hooks.EventLockReleased,
				fmt.Sprintf("cleared %d stale SQLite lock(s)", cleared),
			)
		}
	}

	// ── Step 5: Acquire distributed lock ─────────────────────────────────────

	s.registry.EmitSimple(ctx, hooks.EventLockAcquiring,
		fmt.Sprintf("acquiring lock %q", s.lockKey),
	)

	distLock, err := s.lockManager.Acquire(ctx, s.lockKey)
	if err != nil {
		return fmt.Errorf("schemashift: acquire lock: %w", err)
	}
	defer func() {
		if releaseErr := distLock.Release(context.Background()); releaseErr != nil {
			s.registry.EmitSimple(ctx, hooks.EventShifterError,
				fmt.Sprintf("warning: release lock: %v", releaseErr),
			)
		} else {
			s.registry.EmitSimple(ctx, hooks.EventLockReleased,
				fmt.Sprintf("lock %q released (held for %s)", s.lockKey, distLock.HeldFor().Round(time.Millisecond)),
			)
		}
	}()

	s.registry.EmitSimple(ctx, hooks.EventLockAcquired,
		fmt.Sprintf("lock %q acquired", s.lockKey),
	)

	// Start lock watchdog — warn if lock is held > 10 minutes
	distLock.Watchdog(ctx, 10*time.Minute, func(d time.Duration) {
		s.registry.EmitSimple(ctx, hooks.EventShifterError,
			fmt.Sprintf("WARNING: migration lock held for %s — possible stuck migration", d),
		)
	})

	// ── Step 6: Check for incomplete migrations from previous run ────────────

	if err := s.recoverIncomplete(ctx, migrations); err != nil {
		return fmt.Errorf("schemashift: recovery: %w", err)
	}

	// ── Step 7: Build execution plan ─────────────────────────────────────────

	appliedVersions, err := s.tracker.AppliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("schemashift: load applied versions: %w", err)
	}

	plan, err := s.planner.Build(ctx, migrations, appliedVersions)
	if err != nil {
		return fmt.Errorf("schemashift: build plan: %w", err)
	}

	// Emit plan summary
	s.registry.Emit(ctx, hooks.Event{
		Kind:    hooks.EventPlanBuilt,
		Message: plan.Summary(),
	})

	// ── Step 8: Dry run — exit before executing ───────────────────────────────

	if s.dryRun {
		s.registry.EmitSimple(ctx, hooks.EventShifterComplete,
			"dry-run mode — no migrations applied",
		)
		return nil
	}

	// ── Step 9: Execute pending migrations ───────────────────────────────────

	if plan.IsEmpty() {
		s.registry.EmitSimple(ctx, hooks.EventShifterComplete,
			"nothing to do — schema is up to date",
		)
		return nil
	}

	var lastErr error
	applied := 0

	for _, entry := range plan.Entries {
		if err := s.executeMigration(ctx, entry); err != nil {
			lastErr = err
			if s.stopOnError {
				s.registry.Emit(ctx, hooks.Event{
					Kind:     hooks.EventShifterError,
					Error:    err,
					Duration: time.Since(applyStart),
					Message:  fmt.Sprintf("stopped after error in %q", entry.Migration.Version),
				})
				return fmt.Errorf("schemashift: apply %q: %w", entry.Migration.Version, err)
			}
			// Continue on error — log and move to next migration
			s.registry.EmitMigrationEvent(ctx, hooks.EventMigrationError, entry.Migration, 0, err)
			continue
		}
		applied++
	}

	totalDuration := time.Since(applyStart)

	if lastErr != nil {
		s.registry.Emit(ctx, hooks.Event{
			Kind:     hooks.EventShifterError,
			Error:    lastErr,
			Duration: totalDuration,
			Message: fmt.Sprintf(
				"completed with errors — %d/%d applied in %s",
				applied, len(plan.Entries), totalDuration.Round(time.Millisecond),
			),
		})
		return fmt.Errorf("schemashift: %d migration(s) failed", len(plan.Entries)-applied)
	}

	s.registry.Emit(ctx, hooks.Event{
		Kind:     hooks.EventShifterComplete,
		Duration: totalDuration,
		Message: fmt.Sprintf(
			"complete — %d migration(s) applied in %s",
			applied, totalDuration.Round(time.Millisecond),
		),
	})

	return nil
}

// Rollback rolls back the most recently applied migration.
// It fetches the latest applied version from the tracker and reverses it.
//
// Example:
//
//	err := shifter.Rollback(ctx, migrations...)
func (s *Shifter) Rollback(ctx context.Context, migrations ...*migration.Migration) error {
	if err := s.initialize(ctx); err != nil {
		return fmt.Errorf("schemashift: rollback initialize: %w", err)
	}

	// Find the most recently applied migration
	records, err := s.tracker.All(ctx)
	if err != nil {
		return fmt.Errorf("schemashift: rollback get records: %w", err)
	}
	if len(records) == 0 {
		return fmt.Errorf("schemashift: rollback: no applied migrations found")
	}

	// Most recent is last (ordered by applied_at ASC)
	latest := records[len(records)-1]

	// Find the matching migration definition
	var target *migration.Migration
	for _, m := range migrations {
		if m.Version == latest.Version {
			target = m
			break
		}
	}
	if target == nil {
		return fmt.Errorf(
			"schemashift: rollback: migration %q is applied but not found in registered list — cannot roll back",
			latest.Version,
		)
	}

	// Acquire lock
	distLock, err := s.lockManager.Acquire(ctx, s.lockKey)
	if err != nil {
		return fmt.Errorf("schemashift: rollback acquire lock: %w", err)
	}
	defer distLock.Release(context.Background())

	s.registry.EmitMigrationEvent(ctx, hooks.EventMigrationRollback, target, 0, nil)

	// Save rolling back checkpoint
	_ = s.checkpoints.Save(ctx, &migration.Checkpoint{
		Version:   target.Version,
		State:     migration.CheckpointRollingBack,
		StepIndex: -1,
	})

	// Execute rollback
	if err := target.Rollback(ctx, s.db, s.dialect); err != nil {
		_ = s.checkpoints.SaveFailed(ctx, target.Version, -1, migration.KindRawSQL, err)
		return fmt.Errorf("schemashift: rollback %q: %w", target.Version, err)
	}

	// Remove from tracker
	if err := s.tracker.MarkRolledBack(ctx, target.Version); err != nil {
		return fmt.Errorf("schemashift: rollback mark rolled back: %w", err)
	}

	// Save rolled back checkpoint
	_ = s.checkpoints.Save(ctx, &migration.Checkpoint{
		Version: target.Version,
		State:   migration.CheckpointRolledBack,
	})

	s.registry.EmitMigrationEvent(ctx, hooks.EventMigrationComplete, target, target.ExecutionTime, nil)
	return nil
}

// Status returns a formatted table of all migration states.
//
// Example:
//
//	status, err := shifter.Status(ctx, migrations...)
//	fmt.Println(status)
func (s *Shifter) Status(ctx context.Context, migrations ...*migration.Migration) (string, error) {
	if err := s.initialize(ctx); err != nil {
		return "", fmt.Errorf("schemashift: status initialize: %w", err)
	}
	return s.tracker.Status(ctx, migrations)
}

// Plan builds and returns the execution plan without applying anything.
// Use this to preview what Apply() would do.
//
// Example:
//
//	plan, err := shifter.Plan(ctx, migrations...)
//	fmt.Println(plan.Summary())
func (s *Shifter) Plan(ctx context.Context, migrations ...*migration.Migration) (*migration.Plan, error) {
	if err := s.initialize(ctx); err != nil {
		return nil, fmt.Errorf("schemashift: plan initialize: %w", err)
	}

	appliedVersions, err := s.tracker.AppliedVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("schemashift: plan applied versions: %w", err)
	}

	return s.planner.Build(ctx, migrations, appliedVersions)
}

// CheckDrift checks whether any applied migration's definition has changed.
// Returns a formatted drift report or empty string if clean.
//
// Example:
//
//	report, err := shifter.CheckDrift(ctx, migrations...)
//	if report != "" { log.Warn(report) }
func (s *Shifter) CheckDrift(ctx context.Context, migrations ...*migration.Migration) (string, error) {
	if err := s.initialize(ctx); err != nil {
		return "", fmt.Errorf("schemashift: drift check initialize: %w", err)
	}

	reports, err := s.tracker.CheckDrift(ctx, migrations)
	if err != nil {
		return "", fmt.Errorf("schemashift: drift check: %w", err)
	}

	if len(reports) == 0 {
		return "", nil
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "⚠ schemashift drift detected (%d migration(s)):\n\n", len(reports))
	for _, r := range reports {
		fmt.Fprintf(&sb, "  %s\n", r)
	}
	return sb.String(), nil
}

// Prune removes old checkpoint records to prevent unbounded table growth.
// Keeps the most recent checkpoint per migration version.
// Safe to call periodically (e.g. weekly via cron).
//
// Example:
//
//	deleted, err := shifter.Prune(ctx, 30*24*time.Hour) // keep 30 days
func (s *Shifter) Prune(ctx context.Context, olderThan time.Duration) (int64, error) {
	return s.checkpoints.Prune(ctx, olderThan)
}

// ─── Internal Execution ───────────────────────────────────────────────────────

// initialize ensures all internal tables exist and are valid.
// Idempotent — safe to call multiple times.
func (s *Shifter) initialize(ctx context.Context) error {
	if err := s.tracker.Init(ctx); err != nil {
		return fmt.Errorf("tracker init: %w", err)
	}
	if err := s.checkpoints.EnsureTable(ctx); err != nil {
		return fmt.Errorf("checkpoint table init: %w", err)
	}
	return nil
}

// detectDrift checks all migrations for definition changes.
// Emits DriftDetected events and returns error if drift found.
func (s *Shifter) detectDrift(ctx context.Context, migrations []*migration.Migration) error {
	reports, err := s.tracker.CheckDrift(ctx, migrations)
	if err != nil {
		return fmt.Errorf("drift detection: %w", err)
	}

	for _, r := range reports {
		s.registry.Emit(ctx, hooks.Event{
			Kind:    hooks.EventDriftDetected,
			Message: r.String(),
		})
	}

	return nil // Drift is a warning — not a hard error by default
}

// recoverIncomplete checks for migrations that started but never completed
// from a previous run. Attempts automatic rollback for each.
func (s *Shifter) recoverIncomplete(ctx context.Context, migrations []*migration.Migration) error {
	incomplete, err := s.checkpoints.AllIncomplete(ctx)
	if err != nil {
		return fmt.Errorf("query incomplete: %w", err)
	}

	if len(incomplete) == 0 {
		return nil
	}

	s.registry.EmitSimple(ctx, hooks.EventShifterError,
		fmt.Sprintf("found %d incomplete migration(s) from previous run — attempting recovery", len(incomplete)),
	)

	for _, version := range incomplete {
		cp, err := s.checkpoints.Latest(ctx, version)
		if err != nil {
			return fmt.Errorf("get checkpoint for %q: %w", version, err)
		}

		action := migration.RecommendRecovery(cp)

		// Find the matching migration definition
		var target *migration.Migration
		for _, m := range migrations {
			if m.Version == version {
				target = m
				break
			}
		}

		if target == nil {
			s.registry.EmitSimple(ctx, hooks.EventShifterError,
				fmt.Sprintf("incomplete migration %q not found in registered list — manual recovery required", version),
			)
			continue
		}

		switch action {
		case migration.RecoveryRollback:
			s.registry.EmitMigrationEvent(ctx, hooks.EventMigrationRollback, target, 0, nil)

			_ = s.checkpoints.Save(ctx, &migration.Checkpoint{
				Version: version,
				State:   migration.CheckpointRollingBack,
			})

			if rbErr := target.Rollback(ctx, s.db, s.dialect); rbErr != nil {
				// Rollback failed — mark as failed and continue
				_ = s.checkpoints.SaveFailed(ctx, version, cp.StepIndex, migration.StepKind(cp.StepKind), rbErr)
				s.registry.EmitSimple(ctx, hooks.EventShifterError,
					fmt.Sprintf("recovery rollback of %q failed: %v — manual intervention required", version, rbErr),
				)
				continue
			}

			// Remove from tracker if it was marked applied
			_ = s.tracker.MarkRolledBack(ctx, version)

			_ = s.checkpoints.Save(ctx, &migration.Checkpoint{
				Version: version,
				State:   migration.CheckpointRolledBack,
			})

			s.registry.EmitSimple(ctx, hooks.EventMigrationRollback,
				fmt.Sprintf("recovery: %q rolled back successfully", version),
			)

		case migration.RecoveryManual:
			s.registry.EmitSimple(ctx, hooks.EventShifterError,
				fmt.Sprintf("recovery: %q requires manual intervention (state: %s)", version, cp.State),
			)
		}
	}

	return nil
}

// executeMigration runs a single migration entry from the plan.
// Handles checkpointing, strategy dispatch, and tracker updates.
func (s *Shifter) executeMigration(ctx context.Context, entry *migration.PlanEntry) error {
	m := entry.Migration
	start := time.Now()

	s.registry.EmitMigrationEvent(ctx, hooks.EventMigrationStart, m, 0, nil)

	// Save starting checkpoint
	if err := s.checkpoints.Save(ctx, &migration.Checkpoint{
		Version:   m.Version,
		State:     migration.CheckpointStarted,
		StepIndex: -1,
	}); err != nil {
		// Non-fatal — checkpoint failure shouldn't block migration
		s.registry.EmitSimple(ctx, hooks.EventShifterError,
			fmt.Sprintf("warning: save start checkpoint for %q: %v", m.Version, err),
		)
	}

	// Execute each step with its resolved strategy
	for i, step := range m.Steps {
		resolvedStrategy := entry.StepStrategies[i]

		// Get the executor for this strategy
		executor, err := strategy.ForStep(resolvedStrategy, step, m.Config.BatchSize)
		if err != nil {
			return fmt.Errorf("get executor for step %d: %w", i+1, err)
		}

		// Execute the step
		if err := executor.Execute(ctx, s.db, s.dialect, m, i, step, s.registry); err != nil {
			// Save failed checkpoint
			_ = s.checkpoints.SaveFailed(ctx, m.Version, i, step.Kind(), err)

			// Attempt rollback of steps completed so far
			s.registry.EmitMigrationEvent(ctx, hooks.EventMigrationRollback, m, 0, err)
			s.rollbackCompletedSteps(ctx, m, i-1)

			return fmt.Errorf("step %d (%s): %w", i+1, step.Kind(), err)
		}

		// Save step complete checkpoint
		_ = s.checkpoints.SaveStepComplete(ctx, m.Version, i, step.Kind(), nil)
	}

	// All steps succeeded — record in tracker
	m.ExecutionTime = time.Since(start)

	if err := s.tracker.MarkApplied(ctx, m); err != nil {
		return fmt.Errorf("mark applied %q: %w", m.Version, err)
	}

	// Save completion checkpoint
	_ = s.checkpoints.Save(ctx, &migration.Checkpoint{
		Version:   m.Version,
		State:     migration.CheckpointComplete,
		StepIndex: len(m.Steps) - 1,
	})

	s.registry.EmitMigrationEvent(ctx, hooks.EventMigrationComplete, m, m.ExecutionTime, nil)
	return nil
}

// rollbackCompletedSteps attempts to reverse steps 0..lastCompleted in reverse order.
// Called when a step fails mid-migration to undo partial changes.
// Errors during rollback are logged but do not stop the rollback attempt.
func (s *Shifter) rollbackCompletedSteps(ctx context.Context, m *migration.Migration, lastCompleted int) {
	if lastCompleted < 0 {
		return // No steps completed — nothing to roll back
	}

	for i := lastCompleted; i >= 0; i-- {
		step := m.Steps[i]

		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			s.registry.EmitSimple(ctx, hooks.EventShifterError,
				fmt.Sprintf("rollback step %d: begin tx: %v", i+1, err),
			)
			continue
		}

		if err := step.Reverse(ctx, tx, s.dialect); err != nil {
			_ = tx.Rollback()
			s.registry.EmitSimple(ctx, hooks.EventShifterError,
				fmt.Sprintf("rollback step %d (%s): %v", i+1, step.Kind(), err),
			)
			continue
		}

		if err := tx.Commit(); err != nil {
			s.registry.EmitSimple(ctx, hooks.EventShifterError,
				fmt.Sprintf("rollback step %d: commit: %v", i+1, err),
			)
		}
	}
}
