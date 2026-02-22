package strategy

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sravankolanu20/schemashift/dialect"
	"github.com/sravankolanu20/schemashift/hooks"
	"github.com/sravankolanu20/schemashift/migration"
)

// Executor is the interface every strategy must implement.
// Each strategy receives a step and executes it in its own way.
type Executor interface {
	// Execute applies the step to the database.
	Execute(
		ctx context.Context,
		db *sql.DB,
		d dialect.Dialect,
		m *migration.Migration,
		stepIndex int,
		step migration.Step,
		registry *hooks.Registry,
	) error
}

// ForStep returns the correct Executor for a given step and resolved strategy.
// This is the central dispatch point — called by the Shifter for every step.
func ForStep(
	hint migration.StrategyHint,
	step migration.Step,
	batchSize int,
) (Executor, error) {

	switch hint {
	case migration.StrategyInPlace:
		return &InPlace{}, nil

	case migration.StrategyShadow:
		return &Shadow{BatchSize: batchSize}, nil

	case migration.StrategyDualWrite:
		return &DualWrite{BatchSize: batchSize}, nil

	case migration.StrategyAuto:
		// StrategyAuto should always be resolved before reaching here.
		// The Planner resolves it — if it reaches ForStep it's a bug.
		return nil, fmt.Errorf(
			"schemashift/strategy: StrategyAuto reached ForStep for step %s — "+
				"planner must resolve strategy before execution",
			step.Kind(),
		)
	}

	return nil, fmt.Errorf(
		"schemashift/strategy: unknown strategy hint %d for step %s",
		hint, step.Kind(),
	)
}

// tableColumns fetches the column names for a table using the dialect's
// introspection query. Used by Shadow and DualWrite to determine which
// columns to copy between old and new tables.
func tableColumns(ctx context.Context, db *sql.DB, d dialect.Dialect, table string) ([]string, error) {
	query := d.GetTableInfoSQL(table)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("schemashift/strategy: get columns for %q: %w", table, err)
	}
	defer rows.Close()

	info, err := d.ParseTableInfo(rows)
	if err != nil {
		return nil, err
	}

	if info == nil || len(info.Columns) == 0 {
		return nil, fmt.Errorf("schemashift/strategy: table %q has no columns or does not exist", table)
	}

	names := make([]string, len(info.Columns))
	for i, col := range info.Columns {
		names[i] = col.Name
	}
	return names, nil
}

// rowCount returns the exact row count for a table.
func rowCount(ctx context.Context, db *sql.DB, d dialect.Dialect, table string) (int64, error) {
	query := d.RowCountSQL(table)
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("schemashift/strategy: row count %q: %w", table, err)
	}
	return count, nil
}

// tableExists checks whether a table exists using the dialect's TableExistsSQL.
func tableExists(ctx context.Context, db *sql.DB, d dialect.Dialect, table string) (bool, error) {
	query := d.TableExistsSQL(table)
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("schemashift/strategy: check table exists %q: %w", table, err)
	}
	return count > 0, nil
}

// shadowTableName returns the shadow table name for a given source table.
// Convention: _shadow_<table>
func shadowTableName(table string) string {
	return fmt.Sprintf("_shadow_%s", table)
}

// backupTableName returns the backup table name used during shadow rename cutover.
// Convention: _backup_<table>
func backupTableName(table string) string {
	return fmt.Sprintf("_backup_%s", table)
}
