package migration

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/SravanKolanu20/schemashift/dialect"
)

// State represents the current execution state of a migration.
type State string

const (
	// StatePending means the migration has not been applied yet.
	StatePending State = "pending"

	// StateRunning means the migration is currently being applied.
	StateRunning State = "running"

	// StateComplete means the migration was applied successfully.
	StateComplete State = "complete"

	// StateFailed means the migration encountered an error and was rolled back.
	StateFailed State = "failed"

	// StateRolledBack means the migration was explicitly rolled back after completion.
	StateRolledBack State = "rolled_back"
)

// Direction indicates whether a migration is being applied or reversed.
type Direction string

const (
	DirectionForward Direction = "forward"
	DirectionReverse Direction = "reverse"
)

// Config holds per-migration configuration that overrides global defaults.
type Config struct {
	// BatchSize controls how many rows are copied per batch in shadow/dualwrite strategies.
	// Defaults to 1000 if zero.
	BatchSize int

	// Timeout is the maximum duration allowed for this entire migration.
	// Defaults to 30 minutes if zero.
	Timeout time.Duration

	// DisableTransaction runs the migration outside a transaction.
	// Required for some DDL operations in MySQL (DDL auto-commits in MySQL).
	// USE WITH CAUTION — failure mid-migration will leave schema in partial state.
	DisableTransaction bool

	// AllowDestructive permits steps that permanently destroy data (DropColumn, DropTable).
	// Defaults to false — schemashift will refuse destructive steps unless explicitly allowed.
	AllowDestructive bool

	// LockKey overrides the advisory lock key for this migration.
	// Defaults to "schemashift_<Version>" if empty.
	LockKey string
}

// DefaultConfig returns a Config with sensible production defaults.
func DefaultConfig() Config {
	return Config{
		BatchSize:          1000,
		Timeout:            30 * time.Minute,
		DisableTransaction: false,
		AllowDestructive:   false,
	}
}

// Migration represents a single versioned schema migration.
// It contains an ordered list of Steps and metadata about when
// and how the migration should be applied.
//
// Example usage:
//
//	m := migration.New("20240101_add_phone_to_users",
//	    migration.WithDescription("Add phone column to users table"),
//	    migration.WithStep(&migration.AddColumnStep{
//	        Table: "users",
//	        Column: dialect.ColumnDef{
//	            Name:     "phone",
//	            Type:     "TEXT",
//	            Nullable: true,
//	        },
//	    }),
//	)
type Migration struct {
	// Version is the unique identifier for this migration.
	// Must be unique across all migrations.
	// Convention: "YYYYMMDDHHMMSS_short_description"
	// Example: "20240101120000_add_phone_to_users"
	Version string

	// Description is a human-readable explanation of what this migration does.
	Description string

	// Steps is the ordered list of operations to perform.
	// Steps are executed in order for Forward, reversed for Reverse.
	Steps []Step

	// Config holds per-migration settings.
	Config Config

	// AppliedAt is set when the migration completes successfully.
	// Zero value means not yet applied.
	AppliedAt *time.Time

	// State tracks current execution state.
	State State

	// ExecutionTime records how long the migration took.
	ExecutionTime time.Duration

	// Error holds the last error encountered, if any.
	Error string
}

// Option is a functional option for configuring a Migration.
type Option func(*Migration)

// New creates a new Migration with the given version and options.
//
// Example:
//
//	m := migration.New("20240101_add_phone",
//	    migration.WithDescription("Add phone to users"),
//	    migration.WithBatchSize(500),
//	    migration.WithStep(&migration.AddColumnStep{...}),
//	)
func New(version string, opts ...Option) *Migration {
	m := &Migration{
		Version: version,
		Config:  DefaultConfig(),
		State:   StatePending,
	}

	// Apply all functional options
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// ─── Functional Options ───────────────────────────────────────────────────────

// WithDescription sets a human-readable description for the migration.
func WithDescription(desc string) Option {
	return func(m *Migration) {
		m.Description = desc
	}
}

// WithStep appends one or more steps to the migration.
// Steps are executed in the order they are added.
func WithStep(steps ...Step) Option {
	return func(m *Migration) {
		m.Steps = append(m.Steps, steps...)
	}
}

// WithBatchSize sets the number of rows copied per batch for shadow/dualwrite strategies.
func WithBatchSize(size int) Option {
	return func(m *Migration) {
		m.Config.BatchSize = size
	}
}

// WithTimeout sets the maximum duration for this migration.
func WithTimeout(d time.Duration) Option {
	return func(m *Migration) {
		m.Config.Timeout = d
	}
}

// WithAllowDestructive permits steps that permanently destroy data.
func WithAllowDestructive() Option {
	return func(m *Migration) {
		m.Config.AllowDestructive = true
	}
}

// WithDisableTransaction runs the migration outside a transaction.
// Required for certain DDL in MySQL. Use with caution.
func WithDisableTransaction() Option {
	return func(m *Migration) {
		m.Config.DisableTransaction = true
	}
}

// WithLockKey overrides the advisory lock key for this migration.
func WithLockKey(key string) Option {
	return func(m *Migration) {
		m.Config.LockKey = key
	}
}

// ─── Lifecycle Methods ────────────────────────────────────────────────────────

// Validate checks that the migration is well-formed before execution.
// Validates version format, step validity, and dialect compatibility.
func (m *Migration) Validate(d dialect.Dialect) error {
	// Version must not be empty
	if strings.TrimSpace(m.Version) == "" {
		return fmt.Errorf("schemashift: migration has empty version")
	}

	// Version must match safe identifier pattern
	if !validVersion.MatchString(m.Version) {
		return fmt.Errorf(
			"schemashift: migration version %q is invalid — use only letters, numbers, and underscores",
			m.Version,
		)
	}

	// Must have at least one step
	if len(m.Steps) == 0 {
		return fmt.Errorf("schemashift: migration %q has no steps", m.Version)
	}

	// Check for destructive steps if not allowed
	for i, step := range m.Steps {
		if !m.Config.AllowDestructive {
			switch step.Kind() {
			case KindDropColumn, KindDropTable:
				return fmt.Errorf(
					"schemashift: migration %q step %d is destructive (%s) — call WithAllowDestructive() to permit it",
					m.Version, i+1, step.Kind(),
				)
			}
		}

		// Validate each step against the dialect
		if err := step.Validate(d); err != nil {
			return fmt.Errorf(
				"schemashift: migration %q step %d (%s) validation failed: %w",
				m.Version, i+1, step.Kind(), err,
			)
		}
	}

	// Batch size must be positive
	if m.Config.BatchSize <= 0 {
		return fmt.Errorf(
			"schemashift: migration %q has invalid batch size %d — must be > 0",
			m.Version, m.Config.BatchSize,
		)
	}

	return nil
}

// Run executes the migration in the Forward direction against the database.
// It wraps all steps in a transaction unless DisableTransaction is set.
// Context carries cancellation — if ctx is cancelled mid-migration,
// the transaction is rolled back automatically.
func (m *Migration) Run(ctx context.Context, db *sql.DB, d dialect.Dialect) error {
	// Apply per-migration timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, m.Config.Timeout)
	defer cancel()

	start := time.Now()
	m.State = StateRunning

	var err error
	if m.Config.DisableTransaction {
		err = m.runWithoutTransaction(timeoutCtx, db, d, DirectionForward)
	} else {
		err = m.runInTransaction(timeoutCtx, db, d, DirectionForward)
	}

	m.ExecutionTime = time.Since(start)

	if err != nil {
		m.State = StateFailed
		m.Error = err.Error()
		return err
	}

	now := time.Now()
	m.AppliedAt = &now
	m.State = StateComplete
	return nil
}

// Rollback reverses the migration by executing all steps in reverse order.
// Each step's Reverse() method is called. Steps that return ErrNotReversible
// cause the rollback to stop and return an error.
func (m *Migration) Rollback(ctx context.Context, db *sql.DB, d dialect.Dialect) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, m.Config.Timeout)
	defer cancel()

	var err error
	if m.Config.DisableTransaction {
		err = m.runWithoutTransaction(timeoutCtx, db, d, DirectionReverse)
	} else {
		err = m.runInTransaction(timeoutCtx, db, d, DirectionReverse)
	}

	if err != nil {
		return fmt.Errorf("schemashift: rollback %q failed: %w", m.Version, err)
	}

	m.State = StateRolledBack
	m.AppliedAt = nil
	return nil
}

// ─── Internal Execution ───────────────────────────────────────────────────────

// runInTransaction wraps step execution in a DB transaction.
// If any step fails, the entire transaction is rolled back automatically.
func (m *Migration) runInTransaction(ctx context.Context, db *sql.DB, d dialect.Dialect, dir Direction) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return fmt.Errorf("schemashift: begin transaction for %q: %w", m.Version, err)
	}

	if err := m.executeSteps(ctx, tx, d, dir); err != nil {
		// Attempt to roll back the transaction
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf(
				"schemashift: step failed (%v) AND transaction rollback failed (%v)",
				err, rbErr,
			)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("schemashift: commit transaction for %q: %w", m.Version, err)
	}

	return nil
}

// runWithoutTransaction executes steps directly on the DB without wrapping in a transaction.
// Used for MySQL DDL which auto-commits, or for long-running migrations
// that cannot hold a transaction open.
func (m *Migration) runWithoutTransaction(ctx context.Context, db *sql.DB, d dialect.Dialect, dir Direction) error {
	// Open a dedicated connection so MySQL GET_LOCK() stays on same connection
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("schemashift: open connection for %q: %w", m.Version, err)
	}
	defer conn.Close()

	// Wrap conn in a fake "tx-like" adapter
	// Steps that need a *sql.Tx still get one — we open per-step mini transactions
	// for DML steps, while DDL steps run directly.
	return m.executeStepsOnConn(ctx, conn, d, dir)
}

// executeSteps runs all steps in the correct order within a transaction.
func (m *Migration) executeSteps(ctx context.Context, tx *sql.Tx, d dialect.Dialect, dir Direction) error {
	steps := m.orderedSteps(dir)

	for i, step := range steps {
		// Check for context cancellation between steps
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"schemashift: migration %q cancelled at step %d/%d (%s): %w",
				m.Version, i+1, len(steps), step.Kind(), ctx.Err(),
			)
		default:
		}

		var err error
		if dir == DirectionForward {
			err = step.Forward(ctx, tx, d)
		} else {
			err = step.Reverse(ctx, tx, d)
		}

		if err != nil {
			return fmt.Errorf(
				"schemashift: migration %q step %d/%d (%s) %s failed: %w",
				m.Version, i+1, len(steps), step.Kind(), dir, err,
			)
		}
	}

	return nil
}

// executeStepsOnConn runs steps on a raw *sql.Conn (no wrapping transaction).
// Each DDL step is executed directly. This handles MySQL's implicit DDL commits.
func (m *Migration) executeStepsOnConn(ctx context.Context, conn *sql.Conn, d dialect.Dialect, dir Direction) error {
	steps := m.orderedSteps(dir)

	for i, step := range steps {
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"schemashift: migration %q cancelled at step %d/%d (%s): %w",
				m.Version, i+1, len(steps), step.Kind(), ctx.Err(),
			)
		default:
		}

		// Open a mini-transaction per step for DML (data) steps
		// DDL steps in MySQL auto-commit anyway so transaction is nominal
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf(
				"schemashift: begin mini-tx for step %d (%s): %w",
				i+1, step.Kind(), err,
			)
		}

		if dir == DirectionForward {
			err = step.Forward(ctx, tx, d)
		} else {
			err = step.Reverse(ctx, tx, d)
		}

		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf(
				"schemashift: migration %q step %d/%d (%s) %s failed: %w",
				m.Version, i+1, len(steps), step.Kind(), dir, err,
			)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf(
				"schemashift: commit mini-tx for step %d (%s): %w",
				i+1, step.Kind(), err,
			)
		}
	}

	return nil
}

// orderedSteps returns steps in execution order.
// Forward: steps[0..n], Reverse: steps[n..0]
func (m *Migration) orderedSteps(dir Direction) []Step {
	if dir == DirectionForward {
		return m.Steps
	}

	// Reverse order for rollback
	reversed := make([]Step, len(m.Steps))
	for i, step := range m.Steps {
		reversed[len(m.Steps)-1-i] = step
	}
	return reversed
}

// ─── Helpers & Accessors ──────────────────────────────────────────────────────

// LockKey returns the advisory lock key for this migration.
// Uses Config.LockKey if set, otherwise derives from version.
func (m *Migration) LockKey() string {
	if m.Config.LockKey != "" {
		return m.Config.LockKey
	}
	return fmt.Sprintf("schemashift_%s", m.Version)
}

// IsPending returns true if the migration has not been applied.
func (m *Migration) IsPending() bool {
	return m.State == StatePending
}

// IsComplete returns true if the migration was applied successfully.
func (m *Migration) IsComplete() bool {
	return m.State == StateComplete
}

// IsFailed returns true if the migration encountered an error.
func (m *Migration) IsFailed() bool {
	return m.State == StateFailed
}

// Summary returns a one-line human-readable summary of the migration state.
func (m *Migration) Summary() string {
	switch m.State {
	case StateComplete:
		return fmt.Sprintf(
			"[✓] %s — %s (%d steps, %s)",
			m.Version, m.Description, len(m.Steps), m.ExecutionTime.Round(time.Millisecond),
		)
	case StateFailed:
		return fmt.Sprintf(
			"[✗] %s — %s (FAILED: %s)",
			m.Version, m.Description, m.Error,
		)
	case StateRunning:
		return fmt.Sprintf(
			"[…] %s — %s (running)",
			m.Version, m.Description,
		)
	case StateRolledBack:
		return fmt.Sprintf(
			"[↩] %s — %s (rolled back)",
			m.Version, m.Description,
		)
	default:
		return fmt.Sprintf(
			"[ ] %s — %s (pending, %d steps)",
			m.Version, m.Description, len(m.Steps),
		)
	}
}

// String implements fmt.Stringer.
func (m *Migration) String() string {
	return m.Summary()
}

// ─── Package-level Validation ─────────────────────────────────────────────────

// validVersion is the regex that migration versions must match.
// Allows letters, numbers, and underscores — no spaces or special characters.
var validVersion = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// ValidateAll validates a slice of migrations for:
//   - No duplicate versions
//   - Each migration passes its own Validate()
//   - Migrations are in ascending lexicographic order (convention)
func ValidateAll(migrations []*Migration, d dialect.Dialect) error {
	seen := make(map[string]bool)

	for i, m := range migrations {
		// Check for duplicates
		if seen[m.Version] {
			return fmt.Errorf(
				"schemashift: duplicate migration version %q at index %d",
				m.Version, i,
			)
		}
		seen[m.Version] = true

		// Validate order (lexicographic — timestamp prefixes enforce this naturally)
		if i > 0 && migrations[i].Version <= migrations[i-1].Version {
			return fmt.Errorf(
				"schemashift: migration %q (index %d) is out of order — must come after %q",
				migrations[i].Version, i, migrations[i-1].Version,
			)
		}

		// Validate the migration itself
		if err := m.Validate(d); err != nil {
			return err
		}
	}

	return nil
}

// Builder provides a fluent API for constructing migrations inline.
// Alternative to using New() with functional options.
//
// Example:
//
//	m := migration.Build("20240101_add_phone").
//	    Describe("Add phone column to users").
//	    Add(&AddColumnStep{Table: "users", Column: dialect.ColumnDef{...}}).
//	    Done()
type Builder struct {
	m *Migration
}

// Build starts a new migration builder with the given version.
func Build(version string) *Builder {
	return &Builder{m: New(version)}
}

// Describe sets the migration description.
func (b *Builder) Describe(desc string) *Builder {
	b.m.Description = desc
	return b
}

// Add appends steps to the migration.
func (b *Builder) Add(steps ...Step) *Builder {
	b.m.Steps = append(b.m.Steps, steps...)
	return b
}

// BatchSize sets the batch size for this migration.
func (b *Builder) BatchSize(size int) *Builder {
	b.m.Config.BatchSize = size
	return b
}

// Timeout sets the execution timeout for this migration.
func (b *Builder) Timeout(d time.Duration) *Builder {
	b.m.Config.Timeout = d
	return b
}

// AllowDestructive permits destructive steps in this migration.
func (b *Builder) AllowDestructive() *Builder {
	b.m.Config.AllowDestructive = true
	return b
}

// Done finalizes and returns the built Migration.
func (b *Builder) Done() *Migration {
	return b.m
}
