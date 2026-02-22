package strategy

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/sravankolanu20/schemashift/dialect"
	"github.com/sravankolanu20/schemashift/hooks"
	"github.com/sravankolanu20/schemashift/migration"
)

// DualWrite implements the dual-write strategy for zero-downtime column changes.
//
// Best for: adding NOT NULL columns, changing column types, backfilling data.
//
// Flow:
//  1. Add the new column (nullable) to the live table
//  2. Backfill existing rows in batches using an expression or default value
//  3. Add NOT NULL constraint / change type after backfill is complete
//
// Unlike Shadow, DualWrite operates on the SAME table — no rename needed.
// The app can write to both old and new columns simultaneously during backfill.
type DualWrite struct {
	// BatchSize is how many rows to backfill per batch.
	BatchSize int
}

// BackfillExpr carries the SQL expression used to populate the new column
// from existing data during the backfill phase.
type BackfillExpr struct {
	// NewColumn is the column being populated.
	NewColumn string

	// Expression is a SQL expression that computes the new value.
	// Example: "CONCAT(first_name, ' ', last_name)"
	// Example: "COALESCE(legacy_field, 'default_value')"
	// Example: "NOW()"
	Expression string

	// WhereClause optionally limits which rows are backfilled.
	// Example: "status = 'active'"
	// Leave empty to backfill all rows.
	WhereClause string
}

// Execute runs the dual-write strategy for a step.
// Currently supports AddColumnStep with backfill expressions.
func (dw *DualWrite) Execute(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	m *migration.Migration,
	stepIndex int,
	step migration.Step,
	registry *hooks.Registry,
) error {

	start := time.Now()

	registry.EmitStepEvent(
		ctx, hooks.EventStepStart,
		m, stepIndex, step.Kind(),
		migration.StrategyDualWrite, 0, nil,
	)

	addStep, ok := step.(*migration.AddColumnStep)
	if !ok {
		return fmt.Errorf(
			"schemashift/dualwrite: DualWrite strategy only supports AddColumnStep, got %T",
			step,
		)
	}

	// ── Phase 1: Add column as nullable ──────────────────────────────────────

	if err := dw.addColumnNullable(ctx, db, d, addStep); err != nil {
		duration := time.Since(start)
		registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyDualWrite, duration, err)
		return err
	}

	// ── Phase 2: Backfill existing rows ──────────────────────────────────────

	backfill := dw.buildBackfillExpr(addStep)
	if backfill != nil {
		totalRows, err := rowCount(ctx, db, d, addStep.Table)
		if err != nil {
			return fmt.Errorf("schemashift/dualwrite: row count: %w", err)
		}

		if err := dw.backfill(ctx, db, d, m, addStep.Table, backfill, totalRows, registry); err != nil {
			duration := time.Since(start)
			registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyDualWrite, duration, err)
			return err
		}
	}

	// ── Phase 3: Add NOT NULL constraint if needed ───────────────────────────

	if !addStep.Column.Nullable && addStep.Column.Default == nil {
		if err := dw.addNotNullConstraint(ctx, db, d, addStep); err != nil {
			duration := time.Since(start)
			registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyDualWrite, duration, err)
			return err
		}
	}

	duration := time.Since(start)
	registry.EmitStepEvent(ctx, hooks.EventStepComplete, m, stepIndex, step.Kind(), migration.StrategyDualWrite, duration, nil)
	return nil
}

// addColumnNullable adds the column as nullable first — safe for all table sizes.
// We temporarily make it nullable even if the final column is NOT NULL,
// because adding NOT NULL without a DEFAULT requires a full table scan.
func (dw *DualWrite) addColumnNullable(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	step *migration.AddColumnStep,
) error {

	// Make a nullable copy of the column definition
	nullableCol := step.Column
	nullableCol.Nullable = true
	nullableCol.PrimaryKey = false // Can't add PK column via DualWrite

	// For MySQL — check column existence first (no IF NOT EXISTS)
	if d.Name() == "mysql" {
		query := d.ColumnExistsSQL(step.Table, step.Column.Name)
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("schemashift/dualwrite: check column: %w", err)
		}
		if count > 0 {
			return nil // Already exists — skip
		}
	}

	addSQL := d.AddColumnSQL(step.Table, nullableCol)
	if _, err := db.ExecContext(ctx, addSQL); err != nil {
		return fmt.Errorf("schemashift/dualwrite: add nullable column %q: %w", step.Column.Name, err)
	}

	return nil
}

// backfill updates existing rows in batches using the backfill expression.
// Each batch is its own transaction to avoid long-running write locks.
func (dw *DualWrite) backfill(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	m *migration.Migration,
	table string,
	expr *BackfillExpr,
	totalRows int64,
	registry *hooks.Registry,
) error {

	batchSize := dw.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	registry.EmitSimple(ctx, hooks.EventShadowCopyStart,
		fmt.Sprintf("backfilling %q.%q (%s rows)", table, expr.NewColumn, formatRows(totalRows)),
	)

	var (
		rowsUpdated   int64
		batchNumber   int
		backfillStart = time.Now()
	)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("schemashift/dualwrite: backfill cancelled: %w", ctx.Err())
		default:
		}

		batchNumber++

		// Build the batch UPDATE SQL
		// Updates batchSize rows where the new column IS NULL
		updateSQL := dw.buildBatchUpdateSQL(d, table, expr, batchSize)

		result, err := db.ExecContext(ctx, updateSQL)
		if err != nil {
			return fmt.Errorf("schemashift/dualwrite: backfill batch %d: %w", batchNumber, err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			affected = int64(batchSize)
		}
		rowsUpdated += affected

		// Emit progress
		elapsed := time.Since(backfillStart)
		rowsPerSec := 0.0
		if elapsed.Seconds() > 0 {
			rowsPerSec = float64(rowsUpdated) / elapsed.Seconds()
		}

		registry.EmitBatchProgress(ctx, m, &hooks.BatchProgress{
			Table:         table,
			ShadowTable:   fmt.Sprintf("%s.%s (backfill)", table, expr.NewColumn),
			RowsCopied:    rowsUpdated,
			TotalRows:     totalRows,
			BatchNumber:   batchNumber,
			BatchSize:     batchSize,
			ElapsedTime:   elapsed,
			RowsPerSecond: rowsPerSec,
		})

		// No more rows to update
		if affected == 0 {
			break
		}

		// All rows updated
		if rowsUpdated >= totalRows {
			break
		}
	}

	registry.EmitSimple(ctx, hooks.EventShadowCopyComplete,
		fmt.Sprintf("backfill complete: %s rows updated in %s",
			formatRows(rowsUpdated),
			time.Since(backfillStart).Round(time.Millisecond),
		),
	)

	return nil
}

// buildBatchUpdateSQL generates the UPDATE SQL for one backfill batch.
// Updates rows where the new column IS NULL, up to batchSize rows at a time.
func (dw *DualWrite) buildBatchUpdateSQL(
	d dialect.Dialect,
	table string,
	expr *BackfillExpr,
	batchSize int,
) string {
	col := dialect.QuoteIdentifier(d, expr.NewColumn)
	tbl := dialect.QuoteIdentifier(d, table)

	where := fmt.Sprintf("%s IS NULL", col)
	if expr.WhereClause != "" {
		where = fmt.Sprintf("%s AND (%s)", where, expr.WhereClause)
	}

	switch d.Name() {
	case "postgres":
		// Postgres supports UPDATE ... WHERE ctid IN (subquery) for batch limiting
		return fmt.Sprintf(`
UPDATE %s SET %s = %s
WHERE ctid IN (
    SELECT ctid FROM %s
    WHERE %s
    LIMIT %d
);`,
			tbl, col, expr.Expression,
			tbl, where, batchSize,
		)

	case "mysql":
		// MySQL supports UPDATE ... LIMIT directly
		return fmt.Sprintf(
			"UPDATE %s SET %s = %s WHERE %s LIMIT %d;",
			tbl, col, expr.Expression, where, batchSize,
		)

	case "sqlite":
		// SQLite supports UPDATE ... WHERE rowid IN (subquery) for batch limiting
		return fmt.Sprintf(`
UPDATE %s SET %s = %s
WHERE rowid IN (
    SELECT rowid FROM %s
    WHERE %s
    LIMIT %d
);`,
			tbl, col, expr.Expression,
			tbl, where, batchSize,
		)
	}

	return ""
}

// addNotNullConstraint adds a NOT NULL constraint after backfill is complete.
// By this point all rows have a non-null value so the constraint is safe.
func (dw *DualWrite) addNotNullConstraint(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	step *migration.AddColumnStep,
) error {

	var sql string

	switch d.Name() {
	case "postgres":
		// Postgres 12+ supports SET NOT NULL without a full table scan
		// if a CHECK constraint already guarantees non-null values
		sql = fmt.Sprintf(
			`ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;`,
			dialect.QuoteIdentifier(d, step.Table),
			dialect.QuoteIdentifier(d, step.Column.Name),
		)

	case "mysql":
		// MySQL requires MODIFY COLUMN with full type definition to add NOT NULL
		sql = fmt.Sprintf(
			"ALTER TABLE %s MODIFY COLUMN %s %s NOT NULL;",
			dialect.QuoteIdentifier(d, step.Table),
			dialect.QuoteIdentifier(d, step.Column.Name),
			step.Column.Type,
		)

	case "sqlite":
		// SQLite cannot add NOT NULL to an existing column at all
		// This should have been caught by validation — return a clear error
		return fmt.Errorf(
			"schemashift/dualwrite: SQLite cannot add NOT NULL constraint to existing column %q — use Shadow strategy instead",
			step.Column.Name,
		)
	}

	if _, err := db.ExecContext(ctx, sql); err != nil {
		return fmt.Errorf(
			"schemashift/dualwrite: add NOT NULL to %q.%q: %w",
			step.Table, step.Column.Name, err,
		)
	}

	return nil
}

// buildBackfillExpr extracts or constructs a BackfillExpr from a step.
// If the column has a Default value, we use that as the backfill expression.
// Returns nil if no backfill is needed (nullable column with no default).
func (dw *DualWrite) buildBackfillExpr(step *migration.AddColumnStep) *BackfillExpr {
	if step.Column.Default == nil {
		return nil // No default — nothing to backfill
	}

	return &BackfillExpr{
		NewColumn:  step.Column.Name,
		Expression: *step.Column.Default,
	}
}
