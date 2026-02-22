package dialect

import (
	"database/sql"
	"fmt"
)

// ColumnDef represents a column definition used when creating or altering tables.
type ColumnDef struct {
	Name          string
	Type          string // e.g. "TEXT", "INTEGER", "BIGINT", "VARCHAR(255)"
	Nullable      bool
	Default       *string // nil means no default
	PrimaryKey    bool
	AutoIncrement bool
	Unique        bool
	References    *ForeignKey // nil means no FK
}

// ForeignKey represents a foreign key reference.
type ForeignKey struct {
	Table    string
	Column   string
	OnDelete string // "CASCADE", "SET NULL", "RESTRICT"
}

// IndexDef represents an index to be created on a table.
type IndexDef struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

// TableInfo holds metadata about an existing table fetched from the DB.
type TableInfo struct {
	Name    string
	Columns []ColumnInfo
	Indexes []IndexInfo
}

// ColumnInfo holds metadata about an existing column.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Default  *string
}

// IndexInfo holds metadata about an existing index.
type IndexInfo struct {
	Name    string
	Columns []string
	Unique  bool
}

// Dialect is the core interface every database driver must implement.
// It is responsible for generating all DDL and introspection SQL.
type Dialect interface {

	// --- Identity ---

	// Name returns the dialect name: "postgres", "mysql", "sqlite"
	Name() string

	// --- DDL Generation ---

	// CreateTableSQL generates a CREATE TABLE IF NOT EXISTS statement.
	CreateTableSQL(table string, columns []ColumnDef) string

	// DropTableSQL generates a DROP TABLE IF EXISTS statement.
	DropTableSQL(table string) string

	// RenameTableSQL generates an ALTER TABLE ... RENAME TO ... statement.
	RenameTableSQL(oldName, newName string) string

	// AddColumnSQL generates an ALTER TABLE ... ADD COLUMN ... statement.
	AddColumnSQL(table string, col ColumnDef) string

	// DropColumnSQL generates an ALTER TABLE ... DROP COLUMN ... statement.
	DropColumnSQL(table, column string) string

	// RenameColumnSQL generates a statement to rename a column.
	RenameColumnSQL(table, oldCol, newCol string) string

	// CreateIndexSQL generates a CREATE [UNIQUE] INDEX statement.
	CreateIndexSQL(idx IndexDef) string

	// DropIndexSQL generates a DROP INDEX statement.
	DropIndexSQL(table, indexName string) string

	// --- Locking ---

	// AcquireLockSQL returns the SQL to acquire an advisory/application-level lock.
	// Returns ("", false) if the dialect does not support advisory locks.
	AcquireLockSQL(lockKey string) (string, bool)

	// ReleaseLockSQL returns the SQL to release the advisory lock.
	// Returns ("", false) if the dialect does not support advisory locks.
	ReleaseLockSQL(lockKey string) (string, bool)

	// --- Introspection ---

	// TableExistsSQL returns a query that yields 1 row if the table exists, 0 rows otherwise.
	TableExistsSQL(table string) string

	// ColumnExistsSQL returns a query that yields 1 row if the column exists in the table.
	ColumnExistsSQL(table, column string) string

	// GetTableInfoSQL returns a query to fetch column metadata for a table.
	GetTableInfoSQL(table string) string

	// ParseTableInfo reads rows from GetTableInfoSQL and returns a TableInfo struct.
	ParseTableInfo(rows *sql.Rows) (*TableInfo, error)

	// RowCountSQL returns a SELECT COUNT(*) query for the given table.
	RowCountSQL(table string) string

	// --- Batch Copy ---

	// BatchCopySQL generates the SQL to copy a batch of rows from src to dst table.
	// offset and limit control the batch window.
	BatchCopySQL(srcTable, dstTable string, columns []string, offset, limit int) string

	// --- Placeholder ---

	// Placeholder returns the query placeholder for the nth parameter (1-indexed).
	// Postgres uses $1, $2... MySQL and SQLite use ?
	Placeholder(n int) string
}

// ---- Shared Helpers (used by all dialect implementations) ----

// QuoteIdentifier wraps an identifier in the appropriate quotes.
// Postgres/SQLite use double quotes, MySQL uses backticks.
func QuoteIdentifier(d Dialect, name string) string {
	switch d.Name() {
	case "mysql":
		return fmt.Sprintf("`%s`", name)
	default:
		return fmt.Sprintf(`"%s"`, name)
	}
}

// BuildColumnDef generates the inline column definition string from a ColumnDef.
// Used inside CREATE TABLE statements.
func BuildColumnDef(d Dialect, col ColumnDef) string {
	q := QuoteIdentifier(d, col.Name) + " " + col.Type

	if col.PrimaryKey {
		if col.AutoIncrement {
			switch d.Name() {
			case "postgres":
				// Postgres uses SERIAL or BIGSERIAL — type should be set to SERIAL
				// AutoIncrement is implicit, so we just mark PRIMARY KEY
				q += " PRIMARY KEY"
			case "mysql":
				q += " PRIMARY KEY AUTO_INCREMENT"
			case "sqlite":
				q += " PRIMARY KEY AUTOINCREMENT"
			}
		} else {
			q += " PRIMARY KEY"
		}
	}

	if !col.Nullable && !col.PrimaryKey {
		q += " NOT NULL"
	}

	if col.Default != nil {
		q += fmt.Sprintf(" DEFAULT %s", *col.Default)
	}

	if col.Unique && !col.PrimaryKey {
		q += " UNIQUE"
	}

	if col.References != nil {
		q += fmt.Sprintf(
			" REFERENCES %s(%s) ON DELETE %s",
			QuoteIdentifier(d, col.References.Table),
			QuoteIdentifier(d, col.References.Column),
			col.References.OnDelete,
		)
	}

	return q
}

// EnsureDefault returns a pointer to a string — convenience for setting column defaults.
func EnsureDefault(val string) *string {
	return &val
}
