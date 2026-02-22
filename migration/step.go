package migration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/SravanKolanu20/schemashift/dialect"
)

// StrategyHint tells the execution engine which migration strategy to use
// for a given step. The engine may override this if the dialect doesn't
// support the requested strategy.
type StrategyHint int

const (
	// StrategyAuto lets schemashift choose the safest strategy automatically.
	// This is the default and recommended setting.
	StrategyAuto StrategyHint = iota

	// StrategyInPlace executes DDL directly on the live table.
	// Only safe for operations that don't lock the table
	// (e.g. ADD COLUMN with DEFAULT in Postgres 11+).
	StrategyInPlace

	// StrategyShadow uses the shadow table approach:
	// create new table → copy data → atomic rename.
	// Safe for any operation but slower on large tables.
	StrategyShadow

	// StrategyDualWrite adds new column, backfills data, then cuts over.
	// Best for column type changes or adding NOT NULL columns.
	StrategyDualWrite
)

// String returns a human-readable name for the strategy hint.
func (s StrategyHint) String() string {
	switch s {
	case StrategyAuto:
		return "auto"
	case StrategyInPlace:
		return "inplace"
	case StrategyShadow:
		return "shadow"
	case StrategyDualWrite:
		return "dualwrite"
	default:
		return "unknown"
	}
}

// StepKind categorizes what type of schema operation a step performs.
// Used for logging, progress reporting, and rollback planning.
type StepKind string

const (
	KindAddColumn    StepKind = "add_column"
	KindDropColumn   StepKind = "drop_column"
	KindRenameColumn StepKind = "rename_column"
	KindCreateIndex  StepKind = "create_index"
	KindDropIndex    StepKind = "drop_index"
	KindCreateTable  StepKind = "create_table"
	KindDropTable    StepKind = "drop_table"
	KindRenameTable  StepKind = "rename_table"
	KindRawSQL       StepKind = "raw_sql"
)

// StepMeta holds shared metadata about a step.
// Embedded in every concrete step type.
type StepMeta struct {
	// Description is a human-readable explanation of what this step does.
	// Shown in logs and migration history.
	Description string

	// Strategy hints which migration strategy to use.
	// Defaults to StrategyAuto.
	Strategy StrategyHint

	// BatchSize controls how many rows are copied per batch in shadow/dualwrite strategies.
	// 0 means use the migration-level default (typically 1000).
	BatchSize int
}

// Step is the core interface every migration step must implement.
// Steps are executed in order within a Migration.
type Step interface {
	// Kind returns what category of operation this step performs.
	Kind() StepKind

	// Meta returns shared metadata about this step.
	Meta() StepMeta

	// Validate checks whether this step is valid for the given dialect.
	// Returns an error describing why the step cannot be executed.
	// Called before execution begins — never during.
	Validate(d dialect.Dialect) error

	// Forward executes the step against the database.
	// ctx carries cancellation and deadline signals.
	// tx is an active transaction — steps must use it, not open their own.
	Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error

	// Reverse undoes the step. Called during rollback.
	// Not all steps are reversible — return ErrNotReversible if so.
	Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error
}

// ErrNotReversible is returned by Reverse() when a step cannot be undone.
var ErrNotReversible = fmt.Errorf("schemashift: step is not reversible")

// ─── Step: AddColumn ─────────────────────────────────────────────────────────

// AddColumnStep adds a new column to an existing table.
//
// Usage:
//
//	migration.AddColumn("users", dialect.ColumnDef{
//	    Name:     "phone",
//	    Type:     "TEXT",
//	    Nullable: true,
//	})
type AddColumnStep struct {
	StepMeta
	Table  string
	Column dialect.ColumnDef
}

func (s *AddColumnStep) Kind() StepKind { return KindAddColumn }
func (s *AddColumnStep) Meta() StepMeta { return s.StepMeta }

// Validate checks SQLite-specific constraints on ADD COLUMN.
func (s *AddColumnStep) Validate(d dialect.Dialect) error {
	if s.Table == "" {
		return fmt.Errorf("schemashift: AddColumnStep missing table name")
	}
	if s.Column.Name == "" {
		return fmt.Errorf("schemashift: AddColumnStep missing column name")
	}
	if s.Column.Type == "" {
		return fmt.Errorf("schemashift: AddColumnStep missing column type for %q", s.Column.Name)
	}

	// SQLite cannot add a NOT NULL column without a DEFAULT value
	if d.Name() == "sqlite" && !s.Column.Nullable && s.Column.Default == nil && !s.Column.PrimaryKey {
		return fmt.Errorf(
			"schemashift/sqlite: cannot ADD COLUMN %q with NOT NULL and no DEFAULT — use Nullable:true or set a Default value",
			s.Column.Name,
		)
	}

	// SQLite cannot add a UNIQUE column inline
	if d.Name() == "sqlite" && s.Column.Unique {
		return fmt.Errorf(
			"schemashift/sqlite: cannot ADD COLUMN %q with UNIQUE constraint — add a separate CreateIndex step instead",
			s.Column.Name,
		)
	}

	return nil
}

func (s *AddColumnStep) Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	// For MySQL, check column existence first since it lacks IF NOT EXISTS
	if d.Name() == "mysql" {
		exists, err := columnExists(ctx, tx, d, s.Table, s.Column.Name)
		if err != nil {
			return err
		}
		if exists {
			return nil // Already applied — idempotent
		}
	}

	sql := d.AddColumnSQL(s.Table, s.Column)
	if _, err := tx.ExecContext(ctx, sql); err != nil {
		return fmt.Errorf("schemashift: AddColumn %q.%q: %w", s.Table, s.Column.Name, err)
	}
	return nil
}

// Reverse drops the column that was added.
func (s *AddColumnStep) Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	sql := d.DropColumnSQL(s.Table, s.Column.Name)
	if _, err := tx.ExecContext(ctx, sql); err != nil {
		return fmt.Errorf("schemashift: rollback AddColumn — drop %q.%q: %w", s.Table, s.Column.Name, err)
	}
	return nil
}

// ─── Step: DropColumn ────────────────────────────────────────────────────────

// DropColumnStep removes a column from an existing table.
//
// WARNING: This is a destructive operation. Data in the column is permanently lost.
// schemashift will warn before executing this step unless Force is true.
//
// Usage:
//
//	migration.DropColumn("users", "legacy_field")
type DropColumnStep struct {
	StepMeta
	Table  string
	Column string

	// Force skips the destructive operation warning.
	// Set to true only when you are certain the column is safe to drop.
	Force bool
}

func (s *DropColumnStep) Kind() StepKind { return KindDropColumn }
func (s *DropColumnStep) Meta() StepMeta { return s.StepMeta }

func (s *DropColumnStep) Validate(d dialect.Dialect) error {
	if s.Table == "" {
		return fmt.Errorf("schemashift: DropColumnStep missing table name")
	}
	if s.Column == "" {
		return fmt.Errorf("schemashift: DropColumnStep missing column name")
	}
	return nil
}

func (s *DropColumnStep) Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.DropColumnSQL(s.Table, s.Column)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: DropColumn %q.%q: %w", s.Table, s.Column, err)
	}
	return nil
}

// Reverse is not possible for DropColumn — data is gone.
func (s *DropColumnStep) Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	return fmt.Errorf(
		"%w: DropColumn %q.%q — column data cannot be recovered",
		ErrNotReversible, s.Table, s.Column,
	)
}

// ─── Step: RenameColumn ──────────────────────────────────────────────────────

// RenameColumnStep renames a column in an existing table.
//
// Usage:
//
//	migration.RenameColumn("users", "phone", "mobile")
type RenameColumnStep struct {
	StepMeta
	Table   string
	OldName string
	NewName string
}

func (s *RenameColumnStep) Kind() StepKind { return KindRenameColumn }
func (s *RenameColumnStep) Meta() StepMeta { return s.StepMeta }

func (s *RenameColumnStep) Validate(d dialect.Dialect) error {
	if s.Table == "" {
		return fmt.Errorf("schemashift: RenameColumnStep missing table name")
	}
	if s.OldName == "" || s.NewName == "" {
		return fmt.Errorf("schemashift: RenameColumnStep missing old or new column name")
	}
	if s.OldName == s.NewName {
		return fmt.Errorf("schemashift: RenameColumnStep old and new names are identical: %q", s.OldName)
	}
	return nil
}

func (s *RenameColumnStep) Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.RenameColumnSQL(s.Table, s.OldName, s.NewName)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: RenameColumn %q.%q → %q: %w", s.Table, s.OldName, s.NewName, err)
	}
	return nil
}

// Reverse renames the column back to its original name.
func (s *RenameColumnStep) Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.RenameColumnSQL(s.Table, s.NewName, s.OldName)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: rollback RenameColumn %q.%q → %q: %w", s.Table, s.NewName, s.OldName, err)
	}
	return nil
}

// ─── Step: CreateIndex ───────────────────────────────────────────────────────

// CreateIndexStep creates an index on one or more columns.
//
// Usage:
//
//	migration.CreateIndex(dialect.IndexDef{
//	    Name:    "idx_users_email",
//	    Table:   "users",
//	    Columns: []string{"email"},
//	    Unique:  true,
//	})
type CreateIndexStep struct {
	StepMeta
	Index dialect.IndexDef
}

func (s *CreateIndexStep) Kind() StepKind { return KindCreateIndex }
func (s *CreateIndexStep) Meta() StepMeta { return s.StepMeta }

func (s *CreateIndexStep) Validate(d dialect.Dialect) error {
	if s.Index.Name == "" {
		return fmt.Errorf("schemashift: CreateIndexStep missing index name")
	}
	if s.Index.Table == "" {
		return fmt.Errorf("schemashift: CreateIndexStep missing table name")
	}
	if len(s.Index.Columns) == 0 {
		return fmt.Errorf("schemashift: CreateIndexStep %q has no columns", s.Index.Name)
	}
	return nil
}

func (s *CreateIndexStep) Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.CreateIndexSQL(s.Index)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: CreateIndex %q: %w", s.Index.Name, err)
	}
	return nil
}

// Reverse drops the index that was created.
func (s *CreateIndexStep) Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.DropIndexSQL(s.Index.Table, s.Index.Name)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: rollback CreateIndex — drop %q: %w", s.Index.Name, err)
	}
	return nil
}

// ─── Step: DropIndex ─────────────────────────────────────────────────────────

// DropIndexStep drops an existing index.
//
// Usage:
//
//	migration.DropIndex("users", "idx_users_legacy")
type DropIndexStep struct {
	StepMeta
	Table     string
	IndexName string
}

func (s *DropIndexStep) Kind() StepKind { return KindDropIndex }
func (s *DropIndexStep) Meta() StepMeta { return s.StepMeta }

func (s *DropIndexStep) Validate(d dialect.Dialect) error {
	if s.IndexName == "" {
		return fmt.Errorf("schemashift: DropIndexStep missing index name")
	}
	return nil
}

func (s *DropIndexStep) Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.DropIndexSQL(s.Table, s.IndexName)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: DropIndex %q: %w", s.IndexName, err)
	}
	return nil
}

// Reverse is not possible — we don't store the original index definition.
func (s *DropIndexStep) Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	return fmt.Errorf(
		"%w: DropIndex %q — original index definition not stored",
		ErrNotReversible, s.IndexName,
	)
}

// ─── Step: CreateTable ───────────────────────────────────────────────────────

// CreateTableStep creates a new table with the given column definitions.
//
// Usage:
//
//	migration.CreateTable("orders", []dialect.ColumnDef{
//	    {Name: "id", Type: "SERIAL", PrimaryKey: true},
//	    {Name: "user_id", Type: "INTEGER", Nullable: false},
//	    {Name: "total", Type: "NUMERIC(10,2)", Nullable: false},
//	})
type CreateTableStep struct {
	StepMeta
	Table   string
	Columns []dialect.ColumnDef
}

func (s *CreateTableStep) Kind() StepKind { return KindCreateTable }
func (s *CreateTableStep) Meta() StepMeta { return s.StepMeta }

func (s *CreateTableStep) Validate(d dialect.Dialect) error {
	if s.Table == "" {
		return fmt.Errorf("schemashift: CreateTableStep missing table name")
	}
	if len(s.Columns) == 0 {
		return fmt.Errorf("schemashift: CreateTableStep %q has no columns", s.Table)
	}
	return nil
}

func (s *CreateTableStep) Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.CreateTableSQL(s.Table, s.Columns)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: CreateTable %q: %w", s.Table, err)
	}
	return nil
}

// Reverse drops the table that was created.
func (s *CreateTableStep) Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.DropTableSQL(s.Table)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: rollback CreateTable — drop %q: %w", s.Table, err)
	}
	return nil
}

// ─── Step: RenameTable ───────────────────────────────────────────────────────

// RenameTableStep renames an existing table.
//
// Usage:
//
//	migration.RenameTable("users", "accounts")
type RenameTableStep struct {
	StepMeta
	OldName string
	NewName string
}

func (s *RenameTableStep) Kind() StepKind { return KindRenameTable }
func (s *RenameTableStep) Meta() StepMeta { return s.StepMeta }

func (s *RenameTableStep) Validate(d dialect.Dialect) error {
	if s.OldName == "" || s.NewName == "" {
		return fmt.Errorf("schemashift: RenameTableStep missing old or new table name")
	}
	if s.OldName == s.NewName {
		return fmt.Errorf("schemashift: RenameTableStep old and new names are identical: %q", s.OldName)
	}
	return nil
}

func (s *RenameTableStep) Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.RenameTableSQL(s.OldName, s.NewName)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: RenameTable %q → %q: %w", s.OldName, s.NewName, err)
	}
	return nil
}

// Reverse renames the table back to its original name.
func (s *RenameTableStep) Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	query := d.RenameTableSQL(s.NewName, s.OldName)
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift: rollback RenameTable %q → %q: %w", s.NewName, s.OldName, err)
	}
	return nil
}

// ─── Step: RawSQL ────────────────────────────────────────────────────────────

// RawSQLStep executes arbitrary SQL directly.
// Use this as an escape hatch when no built-in step covers your need.
//
// If you provide ReverseSQL, rollback is supported.
// Otherwise Reverse() returns ErrNotReversible.
//
// Usage:
//
//	migration.RawSQL(
//	    "UPDATE users SET status = 'active' WHERE status IS NULL",
//	    "UPDATE users SET status = NULL WHERE status = 'active'", // optional reverse
//	)
type RawSQLStep struct {
	StepMeta
	SQL        string
	ReverseSQL string // Optional — leave empty if not reversible
}

func (s *RawSQLStep) Kind() StepKind { return KindRawSQL }
func (s *RawSQLStep) Meta() StepMeta { return s.StepMeta }

func (s *RawSQLStep) Validate(d dialect.Dialect) error {
	if strings.TrimSpace(s.SQL) == "" {
		return fmt.Errorf("schemashift: RawSQLStep has empty SQL")
	}
	return nil
}

func (s *RawSQLStep) Forward(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	if _, err := tx.ExecContext(ctx, s.SQL); err != nil {
		return fmt.Errorf("schemashift: RawSQL forward: %w", err)
	}
	return nil
}

func (s *RawSQLStep) Reverse(ctx context.Context, tx *sql.Tx, d dialect.Dialect) error {
	if strings.TrimSpace(s.ReverseSQL) == "" {
		return fmt.Errorf("%w: RawSQL step has no reverse SQL defined", ErrNotReversible)
	}
	if _, err := tx.ExecContext(ctx, s.ReverseSQL); err != nil {
		return fmt.Errorf("schemashift: RawSQL reverse: %w", err)
	}
	return nil
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

// columnExists checks whether a column exists in a table using the dialect's
// ColumnExistsSQL query. Used to implement idempotent ADD COLUMN for MySQL.
func columnExists(ctx context.Context, tx *sql.Tx, d dialect.Dialect, table, column string) (bool, error) {
	query := d.ColumnExistsSQL(table, column)
	var count int
	if err := tx.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("schemashift: check column exists %q.%q: %w", table, column, err)
	}
	return count > 0, nil
}

// strings is used internally — import at top of file
var _ = strings.TrimSpace // ensure strings import is used
