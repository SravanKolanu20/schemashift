package strategy

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/SravanKolanu20/schemashift/dialect"
	"github.com/SravanKolanu20/schemashift/hooks"
	"github.com/SravanKolanu20/schemashift/migration"
)

// InPlace executes a step directly on the live table using standard DDL.
// This is the fastest strategy — no data copying needed.
// Only safe for operations that don't cause long table locks:
//
//   - ADD COLUMN (nullable, or with default in Postgres 11+)
//   - DROP COLUMN (Postgres marks invisible instantly)
//   - RENAME COLUMN (metadata-only change)
//   - CREATE INDEX (use CONCURRENTLY in Postgres for large tables)
//   - DROP INDEX
//   - CREATE TABLE (new empty table — always safe)
//   - RENAME TABLE (atomic metadata operation)
//   - RAW SQL (caller's responsibility)
type InPlace struct{}

// Execute runs the step directly in a transaction.
func (ip *InPlace) Execute(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	m *migration.Migration,
	stepIndex int,
	step migration.Step,
	registry *hooks.Registry,
) error {

	start := time.Now()

	// Emit step start
	registry.EmitStepEvent(
		ctx,
		hooks.EventStepStart,
		m, stepIndex,
		step.Kind(),
		migration.StrategyInPlace,
		0, nil,
	)

	// For CREATE INDEX on Postgres we use CONCURRENTLY which cannot
	// run inside a transaction — handle as special case
	if step.Kind() == migration.KindCreateIndex && d.Name() == "postgres" {
		if err := ip.executePostgresConcurrentIndex(ctx, db, d, m, stepIndex, step, registry); err != nil {
			duration := time.Since(start)
			registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyInPlace, duration, err)
			return err
		}
		duration := time.Since(start)
		registry.EmitStepEvent(ctx, hooks.EventStepComplete, m, stepIndex, step.Kind(), migration.StrategyInPlace, duration, nil)
		return nil
	}

	// Standard path — execute inside a transaction
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("schemashift/inplace: begin tx: %w", err)
	}

	if err := step.Forward(ctx, tx, d); err != nil {
		_ = tx.Rollback()
		duration := time.Since(start)
		registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyInPlace, duration, err)
		return fmt.Errorf("schemashift/inplace: step %d (%s): %w", stepIndex+1, step.Kind(), err)
	}

	if err := tx.Commit(); err != nil {
		duration := time.Since(start)
		registry.EmitStepEvent(ctx, hooks.EventStepError, m, stepIndex, step.Kind(), migration.StrategyInPlace, duration, err)
		return fmt.Errorf("schemashift/inplace: commit step %d: %w", stepIndex+1, err)
	}

	duration := time.Since(start)
	registry.EmitStepEvent(ctx, hooks.EventStepComplete, m, stepIndex, step.Kind(), migration.StrategyInPlace, duration, nil)
	return nil
}

// executePostgresConcurrentIndex runs CREATE INDEX CONCURRENTLY outside
// a transaction — required by Postgres for non-blocking index builds.
// CONCURRENTLY allows reads and writes during index construction at the
// cost of taking longer and not being transactional.
func (ip *InPlace) executePostgresConcurrentIndex(
	ctx context.Context,
	db *sql.DB,
	d dialect.Dialect,
	m *migration.Migration,
	stepIndex int,
	step migration.Step,
	registry *hooks.Registry,
) error {

	indexStep, ok := step.(*migration.CreateIndexStep)
	if !ok {
		return fmt.Errorf("schemashift/inplace: expected CreateIndexStep, got %T", step)
	}

	// Build a CONCURRENTLY version of the CREATE INDEX SQL
	// We do this by modifying the dialect output to inject CONCURRENTLY
	baseSQL := d.CreateIndexSQL(indexStep.Index)
	concurrentSQL := injectConcurrently(baseSQL)

	registry.EmitSimple(ctx, hooks.EventStepStart,
		fmt.Sprintf("CREATE INDEX CONCURRENTLY %q on %q (non-blocking)",
			indexStep.Index.Name, indexStep.Index.Table,
		),
	)

	// Execute directly on the DB (not in a transaction)
	if _, err := db.ExecContext(ctx, concurrentSQL); err != nil {
		return fmt.Errorf("schemashift/inplace: CREATE INDEX CONCURRENTLY %q: %w", indexStep.Index.Name, err)
	}

	return nil
}

// injectConcurrently rewrites "CREATE INDEX IF NOT EXISTS" →
// "CREATE INDEX CONCURRENTLY IF NOT EXISTS" for Postgres.
func injectConcurrently(sql string) string {
	// Handle both UNIQUE and non-UNIQUE variants
	replacements := []struct{ old, new string }{
		{"CREATE UNIQUE INDEX IF NOT EXISTS", "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS"},
		{"CREATE INDEX IF NOT EXISTS", "CREATE INDEX CONCURRENTLY IF NOT EXISTS"},
		{"CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX CONCURRENTLY"},
		{"CREATE INDEX", "CREATE INDEX CONCURRENTLY"},
	}
	for _, r := range replacements {
		if len(sql) >= len(r.old) {
			for i := 0; i <= len(sql)-len(r.old); i++ {
				if sql[i:i+len(r.old)] == r.old {
					return sql[:i] + r.new + sql[i+len(r.old):]
				}
			}
		}
	}
	return sql
}
