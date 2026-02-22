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

// Shadow implements the shadow table strategy for zero-downtime schema changes.
//
// Flow:
//  1. Create shadow table with new schema
//  2. Copy data in batches from live → shadow (idempotent, restartable)
//  3. Atomic rename: live → backup, shadow → live
//  4. Drop backup table
//
// The live table is never locked during data copy.
// Only the final rename requires a brief metadata lock.
type Shadow struct {
	// BatchSize is how many rows to copy per batch.
	BatchSize int
}

// Execute runs the shadow table strategy for a single step.
func (s *Shadow) Execute(
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
		migration.StrategyShadow, 0, nil,
	)

	// Determine the table this step operates on
	table, err := extractTable(step)
	if err != nil {
		return fmt.Errorf("schemashift/shadow: %w", err)
	}

	shadowTable := shadowTableName(table)
	backupTable := backupTableName(table)

	// ── Phase 1: Create shadow table ─────────────────────────────────────────

	if err := s.createShadowTable(ctx, db, d, m, table, shadowTable, step); err != nil {
		registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyShadow, time.Since(start), err)
		return err
	}

	// ── Phase 2: Batch copy data ──────────────────────────────────────────────

	columns, err := tableColumns(ctx, db, d, table)
	if err != nil {
		return fmt.Errorf("schemashift/shadow: get columns: %w", err)
	}

	// Intersect columns — only copy columns that exist in BOTH tables
	// (shadow table may have new columns not in old table, or dropped columns)
	shadowCols, err := tableColumns(ctx, db, d, shadowTable)
	if err != nil {
		return fmt.Errorf("schemashift/shadow: get shadow columns: %w", err)
	}
	copyColumns := intersectColumns(columns, shadowCols)

	totalRows, err := rowCount(ctx, db, d, table)
	if err != nil {
		return fmt.Errorf("schemashift/shadow: row count: %w", err)
	}

	if err := s.batchCopy(ctx, db, d, m, table, shadowTable, copyColumns, totalRows, registry); err != nil {
		registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyShadow, time.Since(start), err)
		return err
	}

	// ── Phase 3: Atomic rename ────────────────────────────────────────────────

	if err := s.atomicRename(ctx, db, d, m, table, shadowTable, backupTable, registry); err != nil {
		registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyShadow, time.Since(start), err)
		return err
	}

	// ── Phase 4: Drop backup table ────────────────────────────────────────────

	if err := s.dropBackup(ctx, db, d, backupTable); err != nil {
		// Non-fatal — backup table is just dead weight, not a correctness issue
		// Log and continue
		registry.EmitSimple(ctx, hooks.EventStepComplete,
			fmt.Sprintf("warning: could not drop backup table %q: %v", backupTable, err),
		)
	}

	duration := time.Since(start)
	registry.EmitStepEvent(ctx, hooks.EventStepComplete, m, stepIndex, step.Kind(), migration.StrategyShadow, duration, nil)
	return nil
}

// createShadowTable creates the shadow table with the new schema.
// The shadow table name is _shadow_<table>.
// If the shadow table already exists (crash recovery), it is reused.
func (s *Shadow) createShadowTable(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	m *migration.Migration,
	table, shadowTable string,
	step migration.Step,
) error {

	// Check if shadow table already exists (resuming after crash)
	exists, err := tableExists(ctx, db, d, shadowTable)
	if err != nil {
		return fmt.Errorf("check shadow table existence: %w", err)
	}
	if exists {
		// Resume from existing shadow table — skip creation
		return nil
	}

	// Get current table info to replicate schema
	query := d.GetTableInfoSQL(table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("get table info for %q: %w", table, err)
	}
	defer rows.Close()

	info, err := d.ParseTableInfo(rows)
	if err != nil {
		return fmt.Errorf("parse table info: %w", err)
	}

	// Build column definitions from existing table info
	cols := make([]dialect.ColumnDef, len(info.Columns))
	for i, col := range info.Columns {
		cols[i] = dialect.ColumnDef{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
			Default:  col.Default,
		}
	}

	// Apply the step's schema change to the column list
	cols = applyStepToColumns(step, cols)

	// Create the shadow table
	createSQL := d.CreateTableSQL(shadowTable, cols)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create shadow table %q: %w", shadowTable, err)
	}

	return nil
}

// batchCopy copies rows from the live table to the shadow table in batches.
// It is fully restartable — ON CONFLICT DO NOTHING / INSERT OR IGNORE
// means already-copied rows are skipped safely.
func (s *Shadow) batchCopy(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	m *migration.Migration,
	srcTable, dstTable string,
	columns []string,
	totalRows int64,
	registry *hooks.Registry,
) error {

	batchSize := s.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	registry.EmitSimple(ctx, hooks.EventShadowCopyStart,
		fmt.Sprintf("starting batch copy: %s → %s (%s rows, batch=%d)",
			srcTable, dstTable, formatRows(totalRows), batchSize,
		),
	)

	var (
		offset      int
		rowsCopied  int64
		batchNumber int
		copyStart   = time.Now()
	)

	for {
		// Check context cancellation between batches
		select {
		case <-ctx.Done():
			return fmt.Errorf("schemashift/shadow: batch copy cancelled: %w", ctx.Err())
		default:
		}

		batchNumber++
		batchStart := time.Now()

		// Generate and execute the batch copy SQL
		batchSQL := d.BatchCopySQL(srcTable, dstTable, columns, offset, batchSize)
		result, err := db.ExecContext(ctx, batchSQL)
		if err != nil {
			return fmt.Errorf("schemashift/shadow: batch %d copy: %w", batchNumber, err)
		}

		// Count rows affected in this batch
		affected, err := result.RowsAffected()
		if err != nil {
			// Some drivers don't support RowsAffected — estimate from batch size
			affected = int64(batchSize)
		}

		rowsCopied += affected
		offset += batchSize

		// Calculate throughput
		elapsed := time.Since(copyStart)
		rowsPerSec := 0.0
		if elapsed.Seconds() > 0 {
			rowsPerSec = float64(rowsCopied) / elapsed.Seconds()
		}

		// Emit progress event
		registry.EmitBatchProgress(ctx, m, &hooks.BatchProgress{
			Table:         srcTable,
			ShadowTable:   dstTable,
			RowsCopied:    rowsCopied,
			TotalRows:     totalRows,
			BatchNumber:   batchNumber,
			BatchSize:     batchSize,
			ElapsedTime:   elapsed,
			RowsPerSecond: rowsPerSec,
		})

		// If this batch copied fewer rows than batchSize, we've reached the end
		if affected < int64(batchSize) {
			break
		}

		// Safety valve — if offset exceeds totalRows + 2 batches, stop
		// (accounts for rows inserted during copy)
		if int64(offset) > totalRows+int64(batchSize*2) {
			break
		}

		_ = batchStart // suppress unused warning
	}

	registry.EmitSimple(ctx, hooks.EventShadowCopyComplete,
		fmt.Sprintf("batch copy complete: %s rows copied in %s",
			formatRows(rowsCopied), time.Since(copyStart).Round(time.Millisecond),
		),
	)

	return nil
}

// atomicRename performs the cutover:
//
//	live table    → backup table
//	shadow table  → live table
//
// For MySQL this is done with a single RENAME TABLE statement that swaps
// both names atomically. For Postgres and SQLite it's two sequential renames
// within a transaction.
func (s *Shadow) atomicRename(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	m *migration.Migration,
	liveTable, shadowTable, backupTable string,
	registry *hooks.Registry,
) error {

	registry.EmitSimple(ctx, hooks.EventShadowRename,
		fmt.Sprintf("renaming: %s → %s, %s → %s",
			liveTable, backupTable, shadowTable, liveTable,
		),
	)

	switch d.Name() {

	case "mysql":
		// MySQL RENAME TABLE supports multiple renames in one atomic statement
		renameSQL := fmt.Sprintf(
			"RENAME TABLE `%s` TO `%s`, `%s` TO `%s`;",
			liveTable, backupTable,
			shadowTable, liveTable,
		)
		if _, err := db.ExecContext(ctx, renameSQL); err != nil {
			return fmt.Errorf("schemashift/shadow: mysql atomic rename: %w", err)
		}

	default: // postgres, sqlite — two renames in one transaction
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return fmt.Errorf("schemashift/shadow: begin rename tx: %w", err)
		}

		// Step 1: live → backup
		if _, err := tx.ExecContext(ctx, d.RenameTableSQL(liveTable, backupTable)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("schemashift/shadow: rename %s → %s: %w", liveTable, backupTable, err)
		}

		// Step 2: shadow → live
		if _, err := tx.ExecContext(ctx, d.RenameTableSQL(shadowTable, liveTable)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("schemashift/shadow: rename %s → %s: %w", shadowTable, liveTable, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("schemashift/shadow: commit rename tx: %w", err)
		}
	}

	return nil
}

// dropBackup drops the backup table after successful cutover.
// Non-fatal — if this fails the schema is still correct, just messy.
func (s *Shadow) dropBackup(ctx context.Context, db *sql.DB, d dialect.Dialect, backupTable string) error {
	dropSQL := d.DropTableSQL(backupTable)
	if _, err := db.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("drop backup table %q: %w", backupTable, err)
	}
	return nil
}

// ─── Shadow Helpers ───────────────────────────────────────────────────────────

// applyStepToColumns modifies a column list based on what a step does.
// This produces the new schema for the shadow table.
func applyStepToColumns(step migration.Step, cols []dialect.ColumnDef) []dialect.ColumnDef {
	switch s := step.(type) {

	case *migration.AddColumnStep:
		// Add the new column to the end
		return append(cols, s.Column)

	case *migration.DropColumnStep:
		// Remove the column by name
		result := cols[:0]
		for _, col := range cols {
			if col.Name != s.Column {
				result = append(result, col)
			}
		}
		return result

	case *migration.RenameColumnStep:
		// Rename the column in place
		for i, col := range cols {
			if col.Name == s.OldName {
				cols[i].Name = s.NewName
			}
		}
		return cols
	}

	// For other step kinds (RawSQL, CreateIndex, etc.) — no column change
	return cols
}

// extractTable returns the primary table name for a step.
// Returns an error if the step kind doesn't operate on a single table.
func extractTable(step migration.Step) (string, error) {
	switch s := step.(type) {
	case *migration.AddColumnStep:
		return s.Table, nil
	case *migration.DropColumnStep:
		return s.Table, nil
	case *migration.RenameColumnStep:
		return s.Table, nil
	case *migration.CreateTableStep:
		return s.Table, nil
	case *migration.RenameTableStep:
		return s.OldName, nil
	}
	return "", fmt.Errorf("step kind %s does not have an extractable table — shadow strategy not applicable", step.Kind())
}

// intersectColumns returns columns that exist in both lists.
// Used to determine which columns to copy between old and shadow tables.
func intersectColumns(a, b []string) []string {
	bSet := make(map[string]bool, len(b))
	for _, col := range b {
		bSet[col] = true
	}
	var result []string
	for _, col := range a {
		if bSet[col] {
			result = append(result, col)
		}
	}
	return result
}

// formatRows formats a row count into a human-readable string.
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
