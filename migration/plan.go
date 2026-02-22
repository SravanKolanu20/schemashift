package migration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/SravanKolanu20/schemashift/dialect"
)

// PlanEntry represents a single migration scheduled for execution
// within an execution plan. It pairs a migration with a resolved
// strategy for each of its steps.
type PlanEntry struct {
	// Migration is the migration to execute.
	Migration *Migration

	// StepStrategies maps step index → resolved StrategyHint.
	// The planner resolves StrategyAuto to a concrete strategy
	// based on the step kind, dialect, and table size.
	StepStrategies []StrategyHint

	// EstimatedRows is the approximate row count for the primary
	// table involved. Used to log expected duration.
	EstimatedRows int64

	// Warnings contains non-fatal notices about this migration entry.
	// Printed to the user before execution begins.
	Warnings []string
}

// Plan is a fully resolved execution plan for a set of migrations.
// It contains only pending migrations in the correct order,
// with strategy decisions already made.
type Plan struct {
	// Entries is the ordered list of migrations to execute.
	Entries []*PlanEntry

	// AlreadyApplied is the count of migrations skipped (already done).
	AlreadyApplied int

	// TotalMigrations is the total number of migrations registered.
	TotalMigrations int

	// CreatedAt is when this plan was built.
	CreatedAt time.Time

	// Dialect is the DB dialect this plan was built for.
	Dialect dialect.Dialect
}

// IsEmpty returns true if there are no pending migrations to run.
func (p *Plan) IsEmpty() bool {
	return len(p.Entries) == 0
}

// Summary returns a multi-line human-readable plan summary.
func (p *Plan) Summary() string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "schemashift execution plan (%s)\n", p.Dialect.Name())
	fmt.Fprintf(&sb, "  total registered : %d\n", p.TotalMigrations)
	fmt.Fprintf(&sb, "  already applied  : %d\n", p.AlreadyApplied)
	fmt.Fprintf(&sb, "  pending          : %d\n", len(p.Entries))
	fmt.Fprintf(&sb, "  built at         : %s\n\n", p.CreatedAt.Format(time.RFC3339))

	if p.IsEmpty() {
		fmt.Fprintf(&sb, "  ✓ nothing to do — schema is up to date\n")
		return sb.String()
	}

	for i, entry := range p.Entries {
		m := entry.Migration
		fmt.Fprintf(&sb, "  [%d] %s\n", i+1, m.Version)
		fmt.Fprintf(&sb, "       %s\n", m.Description)
		fmt.Fprintf(&sb, "       steps: %d\n", len(m.Steps))

		if entry.EstimatedRows > 0 {
			fmt.Fprintf(&sb, "       ~rows: %s\n", formatRows(entry.EstimatedRows))
		}

		for j, step := range m.Steps {
			strategy := entry.StepStrategies[j]
			fmt.Fprintf(&sb, "         step %d: %-20s → strategy: %s\n",
				j+1, step.Kind(), strategy,
			)
		}

		for _, warn := range entry.Warnings {
			fmt.Fprintf(&sb, "       ⚠ WARNING: %s\n", warn)
		}

		fmt.Fprintln(&sb)
	}

	return sb.String()
}

// ─── Planner ──────────────────────────────────────────────────────────────────

// Planner builds execution plans by comparing registered migrations
// against the applied migration history in the tracker table.
type Planner struct {
	db      *sql.DB
	dialect dialect.Dialect

	// largeTableThreshold is the row count above which
	// StrategyAuto prefers shadow table over in-place DDL.
	// Defaults to 100,000 rows.
	largeTableThreshold int64
}

// NewPlanner creates a new Planner.
func NewPlanner(db *sql.DB, d dialect.Dialect) *Planner {
	return &Planner{
		db:                  db,
		dialect:             d,
		largeTableThreshold: 100_000,
	}
}

// WithLargeTableThreshold overrides the row threshold for auto strategy selection.
func (p *Planner) WithLargeTableThreshold(n int64) *Planner {
	p.largeTableThreshold = n
	return p
}

// Build takes all registered migrations and the set of already-applied
// version strings, then constructs a fully resolved Plan.
//
// appliedVersions is typically fetched from the tracker table.
// It is a set (map[string]bool) for O(1) lookup.
func (p *Planner) Build(
	ctx context.Context,
	migrations []*Migration,
	appliedVersions map[string]bool,
) (*Plan, error) {

	plan := &Plan{
		TotalMigrations: len(migrations),
		AlreadyApplied:  0,
		CreatedAt:       time.Now(),
		Dialect:         p.dialect,
	}

	for _, m := range migrations {
		// Skip already-applied migrations
		if appliedVersions[m.Version] {
			plan.AlreadyApplied++
			continue
		}

		// Build a PlanEntry for this pending migration
		entry, err := p.buildEntry(ctx, m)
		if err != nil {
			return nil, fmt.Errorf(
				"schemashift/planner: build entry for %q: %w",
				m.Version, err,
			)
		}

		plan.Entries = append(plan.Entries, entry)
	}

	return plan, nil
}

// buildEntry resolves strategy for each step in the migration and
// estimates row counts for the relevant tables.
func (p *Planner) buildEntry(ctx context.Context, m *Migration) (*PlanEntry, error) {
	entry := &PlanEntry{
		Migration:      m,
		StepStrategies: make([]StrategyHint, len(m.Steps)),
	}

	for i, step := range m.Steps {
		resolved, warn, err := p.resolveStrategy(ctx, step, m.Config)
		if err != nil {
			return nil, fmt.Errorf("step %d (%s): %w", i+1, step.Kind(), err)
		}

		entry.StepStrategies[i] = resolved

		if warn != "" {
			entry.Warnings = append(entry.Warnings, warn)
		}
	}

	// Estimate rows for the primary table (first step that references a table)
	primaryTable := extractPrimaryTable(m.Steps)
	if primaryTable != "" {
		rows, err := p.estimateRows(ctx, primaryTable)
		if err == nil {
			entry.EstimatedRows = rows
		}
		// Non-fatal — row estimation failure doesn't block migration
	}

	return entry, nil
}

// resolveStrategy takes a step and returns the concrete StrategyHint to use.
// StrategyAuto is resolved based on:
//   - Step kind (some ops are always safe in-place)
//   - Dialect capabilities
//   - Table size (large tables → shadow strategy)
//
// Returns the resolved strategy, an optional warning string, and any error.
func (p *Planner) resolveStrategy(
	ctx context.Context,
	step Step,
	cfg Config,
) (StrategyHint, string, error) {

	hint := step.Meta().Strategy

	// If user explicitly chose a strategy, respect it
	if hint != StrategyAuto {
		warn := p.validateExplicitStrategy(hint, step)
		return hint, warn, nil
	}

	// Auto-resolve based on step kind and dialect
	return p.autoResolve(ctx, step, cfg)
}

// autoResolve implements the automatic strategy selection logic.
func (p *Planner) autoResolve(
	ctx context.Context,
	step Step,
	cfg Config,
) (StrategyHint, string, error) {

	kind := step.Kind()
	d := p.dialect.Name()

	switch kind {

	// ── Always safe in-place ──────────────────────────────────────────────────

	case KindCreateTable:
		// Creating a new empty table is always in-place safe
		return StrategyInPlace, "", nil

	case KindCreateIndex:
		// Postgres supports CONCURRENT index creation (non-blocking)
		// MySQL and SQLite create indexes without table locks for most cases
		// We use in-place but add a warning for large tables
		warn := ""
		if table := extractIndexTable(step); table != "" {
			rows, err := p.estimateRows(ctx, table)
			if err == nil && rows > p.largeTableThreshold {
				warn = fmt.Sprintf(
					"CreateIndex on %q has ~%s rows — consider running during low-traffic period",
					table, formatRows(rows),
				)
			}
		}
		return StrategyInPlace, warn, nil

	case KindDropIndex:
		// Dropping an index is generally fast and non-blocking
		return StrategyInPlace, "", nil

	case KindRenameTable:
		// RENAME TABLE is atomic in all three dialects
		return StrategyInPlace, "", nil

	case KindRawSQL:
		// Raw SQL is always in-place — user controls it
		return StrategyInPlace, "", nil

	// ── Requires analysis ─────────────────────────────────────────────────────

	case KindAddColumn:
		return p.resolveAddColumn(ctx, step, d)

	case KindDropColumn:
		return p.resolveDropColumn(ctx, step, d)

	case KindRenameColumn:
		return p.resolveRenameColumn(ctx, step, d)
	}

	// Default fallback — safe but slow
	return StrategyShadow, "", nil
}

// resolveAddColumn picks the best strategy for ADD COLUMN.
//
// Decision tree:
//
//	Postgres 11+ + nullable + no default  → InPlace (instant, no table rewrite)
//	Postgres 11+ + has default            → InPlace (Postgres 11 stores default in catalog)
//	Postgres < 11 + has default           → Shadow  (old Postgres rewrites the whole table)
//	MySQL + nullable                      → InPlace (online DDL)
//	MySQL + NOT NULL + no default         → Shadow  (requires table rebuild)
//	SQLite                                → InPlace (with validation already done in step)
//	Large table (any dialect)             → Shadow  (avoid long lock)
func (p *Planner) resolveAddColumn(ctx context.Context, step Step, dialectName string) (StrategyHint, string, error) {
	addStep, ok := step.(*AddColumnStep)
	if !ok {
		return StrategyShadow, "", nil
	}

	// Estimate rows on the target table
	rows, rowErr := p.estimateRows(ctx, addStep.Table)
	isLarge := rowErr == nil && rows > p.largeTableThreshold

	switch dialectName {

	case "postgres":
		if isLarge {
			return StrategyShadow,
				fmt.Sprintf("large table %q (~%s rows) — using shadow strategy for ADD COLUMN", addStep.Table, formatRows(rows)),
				nil
		}
		// Postgres 11+ handles all ADD COLUMN cases efficiently in-place
		return StrategyInPlace, "", nil

	case "mysql":
		if !addStep.Column.Nullable && addStep.Column.Default == nil {
			// MySQL must rebuild the table for NOT NULL columns without DEFAULT
			return StrategyShadow,
				fmt.Sprintf("ADD COLUMN %q is NOT NULL with no DEFAULT — using shadow table rebuild", addStep.Column.Name),
				nil
		}
		if isLarge {
			return StrategyShadow,
				fmt.Sprintf("large table %q (~%s rows) — using shadow strategy for ADD COLUMN", addStep.Table, formatRows(rows)),
				nil
		}
		return StrategyInPlace, "", nil

	case "sqlite":
		// SQLite ADD COLUMN is always in-place (step.Validate already checked constraints)
		return StrategyInPlace, "", nil
	}

	return StrategyShadow, "", nil
}

// resolveDropColumn picks the best strategy for DROP COLUMN.
//
// DROP COLUMN is always potentially destructive.
// For large tables we prefer shadow (create new table without the column,
// copy data, rename) to avoid long exclusive locks.
func (p *Planner) resolveDropColumn(ctx context.Context, step Step, dialectName string) (StrategyHint, string, error) {
	dropStep, ok := step.(*DropColumnStep)
	if !ok {
		return StrategyShadow, "", nil
	}

	rows, rowErr := p.estimateRows(ctx, dropStep.Table)
	isLarge := rowErr == nil && rows > p.largeTableThreshold

	if isLarge {
		return StrategyShadow,
			fmt.Sprintf("large table %q (~%s rows) — using shadow strategy for DROP COLUMN", dropStep.Table, formatRows(rows)),
			nil
	}

	switch dialectName {
	case "postgres":
		// Postgres DROP COLUMN is instant (marks column invisible, reclaimed by VACUUM)
		return StrategyInPlace, "", nil
	case "mysql":
		// MySQL DROP COLUMN requires table rebuild — use shadow for safety
		return StrategyShadow, "", nil
	case "sqlite":
		// SQLite 3.35+ supports DROP COLUMN in-place
		return StrategyInPlace, "", nil
	}

	return StrategyShadow, "", nil
}

// resolveRenameColumn picks the best strategy for RENAME COLUMN.
// Rename is generally safe in-place across all dialects for small tables.
// For large tables we still prefer shadow to avoid any potential lock.
func (p *Planner) resolveRenameColumn(ctx context.Context, step Step, dialectName string) (StrategyHint, string, error) {
	renameStep, ok := step.(*RenameColumnStep)
	if !ok {
		return StrategyShadow, "", nil
	}

	rows, rowErr := p.estimateRows(ctx, renameStep.Table)
	isLarge := rowErr == nil && rows > p.largeTableThreshold

	if isLarge {
		return StrategyShadow,
			fmt.Sprintf("large table %q (~%s rows) — using shadow strategy for RENAME COLUMN", renameStep.Table, formatRows(rows)),
			nil
	}

	// All three dialects support RENAME COLUMN in-place for small tables
	return StrategyInPlace, "", nil
}

// validateExplicitStrategy checks whether the user-requested strategy
// is compatible with the step kind and dialect. Returns a warning if not ideal.
func (p *Planner) validateExplicitStrategy(hint StrategyHint, step Step) string {
	// Warn if user forces InPlace on a step that typically needs Shadow
	if hint == StrategyInPlace {
		switch step.Kind() {
		case KindDropColumn:
			return fmt.Sprintf(
				"StrategyInPlace forced on DropColumn — this may acquire an exclusive lock on large tables",
			)
		}
	}

	// Warn if user forces Shadow on a step that's always safe in-place
	if hint == StrategyShadow {
		switch step.Kind() {
		case KindCreateTable, KindDropIndex, KindRenameTable:
			return fmt.Sprintf(
				"StrategyShadow on %s is unnecessary — this operation is always fast in-place",
				step.Kind(),
			)
		}
	}

	return ""
}

// ─── Row Estimation ───────────────────────────────────────────────────────────

// estimateRows returns an approximate row count for the given table.
// Uses SELECT COUNT(*) which is exact but may be slow on very large tables.
// For Postgres, a faster estimate from pg_class.reltuples could be used
// in future — for now we keep it simple and consistent across dialects.
func (p *Planner) estimateRows(ctx context.Context, table string) (int64, error) {
	query := p.dialect.RowCountSQL(table)

	var count int64
	err := p.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("schemashift/planner: estimate rows for %q: %w", table, err)
	}
	return count, nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// extractPrimaryTable returns the name of the first table referenced
// by any step in the migration. Used for row count estimation in PlanEntry.
func extractPrimaryTable(steps []Step) string {
	for _, step := range steps {
		switch s := step.(type) {
		case *AddColumnStep:
			return s.Table
		case *DropColumnStep:
			return s.Table
		case *RenameColumnStep:
			return s.Table
		case *CreateTableStep:
			return s.Table
		case *RenameTableStep:
			return s.OldName
		}
	}
	return ""
}

// extractIndexTable returns the table name from a CreateIndexStep.
func extractIndexTable(step Step) string {
	if s, ok := step.(*CreateIndexStep); ok {
		return s.Index.Table
	}
	return ""
}

// formatRows formats a row count into a human-readable string.
// Examples: 1,234  |  12,345  |  1.2M  |  45.6M
func formatRows(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// ─── Plan Diff ────────────────────────────────────────────────────────────────

// Diff compares two plans and returns the set of versions that are
// in newPlan but not in oldPlan — i.e. newly pending migrations.
// Useful for detecting drift between environments.
func Diff(oldApplied, newApplied map[string]bool, all []*Migration) []string {
	var missing []string
	for _, m := range all {
		if oldApplied[m.Version] && !newApplied[m.Version] {
			missing = append(missing, m.Version)
		}
	}
	return missing
}

// PendingVersions returns the list of version strings that are
// registered but not yet applied, in order.
func PendingVersions(migrations []*Migration, applied map[string]bool) []string {
	var pending []string
	for _, m := range migrations {
		if !applied[m.Version] {
			pending = append(pending, m.Version)
		}
	}
	return pending
}

// AppliedVersions returns the list of version strings that are
// registered AND applied, in order.
func AppliedVersions(migrations []*Migration, applied map[string]bool) []string {
	var done []string
	for _, m := range migrations {
		if applied[m.Version] {
			done = append(done, m.Version)
		}
	}
	return done
}
