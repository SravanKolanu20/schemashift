package dialect

import (
	"database/sql"
	"fmt"
	"strings"
)

// SQLiteDialect implements Dialect for SQLite (v3.35+).
// SQLite has significant DDL limitations compared to Postgres and MySQL:
//
//  1. No advisory locks (we simulate with a lock table)
//  2. No DROP COLUMN before SQLite 3.35.0 (2021-03-12)
//  3. No RENAME COLUMN before SQLite 3.25.0 (2018-09-15)
//  4. No ADD COLUMN with constraints (NOT NULL without DEFAULT, UNIQUE, FK)
//  5. AUTOINCREMENT keyword (not AUTO_INCREMENT like MySQL)
//  6. Uses ? placeholders like MySQL
type SQLiteDialect struct{}

// NewSQLite returns a new SQLiteDialect instance.
func NewSQLite() *SQLiteDialect {
	return &SQLiteDialect{}
}

// ─── Identity ────────────────────────────────────────────────────────────────

func (s *SQLiteDialect) Name() string {
	return "sqlite"
}

// ─── DDL Generation ──────────────────────────────────────────────────────────

// CreateTableSQL generates CREATE TABLE IF NOT EXISTS.
// SQLite infers storage class from type affinity, so types are flexible.
//
// Example output:
//
//	CREATE TABLE IF NOT EXISTS "users" (
//	    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
//	    "email" TEXT NOT NULL UNIQUE,
//	    "created_at" TEXT DEFAULT (datetime('now'))
//	);
func (s *SQLiteDialect) CreateTableSQL(table string, columns []ColumnDef) string {
	var cols []string
	for _, col := range columns {
		cols = append(cols, "    "+BuildColumnDef(s, col))
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n%s\n);",
		QuoteIdentifier(s, table),
		strings.Join(cols, ",\n"),
	)
}

// DropTableSQL generates DROP TABLE IF EXISTS.
//
// Example output:
//
//	DROP TABLE IF EXISTS "users";
func (s *SQLiteDialect) DropTableSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", QuoteIdentifier(s, table))
}

// RenameTableSQL generates ALTER TABLE ... RENAME TO ...
// This is one of the few ALTER TABLE operations SQLite has always supported.
//
// Example output:
//
//	ALTER TABLE "users" RENAME TO "users_old";
func (s *SQLiteDialect) RenameTableSQL(oldName, newName string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s RENAME TO %s;",
		QuoteIdentifier(s, oldName),
		QuoteIdentifier(s, newName),
	)
}

// AddColumnSQL generates ALTER TABLE ... ADD COLUMN ...
//
// IMPORTANT SQLite Constraints on ADD COLUMN:
//   - The column CANNOT have a UNIQUE or PRIMARY KEY constraint
//   - The column CANNOT have a NOT NULL constraint unless it has a DEFAULT value
//   - The column CANNOT have a foreign key constraint (unless FK enforcement is off)
//
// The caller (strategy layer) is responsible for ensuring these rules are met.
// schemashift will warn if a violation is detected before executing.
//
// Example output:
//
//	ALTER TABLE "users" ADD COLUMN "phone" TEXT;
func (s *SQLiteDialect) AddColumnSQL(table string, col ColumnDef) string {
	return fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN %s;",
		QuoteIdentifier(s, table),
		BuildColumnDef(s, col),
	)
}

// DropColumnSQL generates ALTER TABLE ... DROP COLUMN ...
// Requires SQLite 3.35.0+ (released March 2021).
// Columns cannot be dropped if they are:
//   - Part of a PRIMARY KEY
//   - Part of an index
//   - Referenced by another table's FK
//   - Used in a trigger or view
//
// Example output:
//
//	ALTER TABLE "users" DROP COLUMN "phone";
func (s *SQLiteDialect) DropColumnSQL(table, column string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s DROP COLUMN %s;",
		QuoteIdentifier(s, table),
		QuoteIdentifier(s, column),
	)
}

// RenameColumnSQL generates ALTER TABLE ... RENAME COLUMN ... TO ...
// Requires SQLite 3.25.0+ (released September 2018).
//
// Example output:
//
//	ALTER TABLE "users" RENAME COLUMN "phone" TO "mobile";
func (s *SQLiteDialect) RenameColumnSQL(table, oldCol, newCol string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s RENAME COLUMN %s TO %s;",
		QuoteIdentifier(s, table),
		QuoteIdentifier(s, oldCol),
		QuoteIdentifier(s, newCol),
	)
}

// CreateIndexSQL generates CREATE [UNIQUE] INDEX IF NOT EXISTS ...
//
// Example output:
//
//	CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");
func (s *SQLiteDialect) CreateIndexSQL(idx IndexDef) string {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}

	quotedCols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		quotedCols[i] = QuoteIdentifier(s, col)
	}

	return fmt.Sprintf(
		"CREATE %sINDEX IF NOT EXISTS %s ON %s (%s);",
		unique,
		QuoteIdentifier(s, idx.Name),
		QuoteIdentifier(s, idx.Table),
		strings.Join(quotedCols, ", "),
	)
}

// DropIndexSQL generates DROP INDEX IF EXISTS ...
// SQLite indexes are not table-scoped (same as Postgres), so table is unused.
//
// Example output:
//
//	DROP INDEX IF EXISTS "idx_users_email";
func (s *SQLiteDialect) DropIndexSQL(table, indexName string) string {
	return fmt.Sprintf("DROP INDEX IF EXISTS %s;", QuoteIdentifier(s, indexName))
}

// ─── Locking ─────────────────────────────────────────────────────────────────

// AcquireLockSQL returns empty string — SQLite does not support advisory locks.
//
// Instead, schemashift uses a dedicated "_schemashift_lock" table with
// an INSERT OR FAIL strategy to simulate exclusive locking for SQLite.
// This is handled entirely in lock/lock.go with SQLite-specific logic.
//
// Returns ("", false) to signal to the lock manager to use table-based locking.
func (s *SQLiteDialect) AcquireLockSQL(lockKey string) (string, bool) {
	return "", false
}

// ReleaseLockSQL returns empty string for same reason as AcquireLockSQL.
func (s *SQLiteDialect) ReleaseLockSQL(lockKey string) (string, bool) {
	return "", false
}

// ─── Introspection ───────────────────────────────────────────────────────────

// TableExistsSQL queries sqlite_master for table existence.
// sqlite_master is SQLite's internal schema catalog.
//
// Example output:
//
//	SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users';
func (s *SQLiteDialect) TableExistsSQL(table string) string {
	return fmt.Sprintf(
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s';",
		escapeSingleQuote(table),
	)
}

// ColumnExistsSQL uses PRAGMA table_info() to check column existence.
// We wrap it in a subquery to get a COUNT(*) like the other dialects.
//
// Example output:
//
//	SELECT COUNT(*) FROM pragma_table_info('users') WHERE name='email';
func (s *SQLiteDialect) ColumnExistsSQL(table, column string) string {
	return fmt.Sprintf(
		"SELECT COUNT(*) FROM pragma_table_info('%s') WHERE name='%s';",
		escapeSingleQuote(table),
		escapeSingleQuote(column),
	)
}

// GetTableInfoSQL uses PRAGMA table_info() to fetch column metadata.
//
// PRAGMA table_info() returns rows with these columns:
//
//	cid | name | type | notnull | dflt_value | pk
//
// Example output:
//
//	PRAGMA table_info('users');
func (s *SQLiteDialect) GetTableInfoSQL(table string) string {
	return fmt.Sprintf("PRAGMA table_info('%s');", escapeSingleQuote(table))
}

// ParseTableInfo reads PRAGMA table_info() rows and builds a TableInfo.
//
// PRAGMA table_info columns:
//
//	0: cid        (int)    - column index
//	1: name       (string) - column name
//	2: type       (string) - declared type
//	3: notnull    (int)    - 1 if NOT NULL
//	4: dflt_value (string) - default value expression or NULL
//	5: pk         (int)    - 1 if part of primary key
func (s *SQLiteDialect) ParseTableInfo(rows *sql.Rows) (*TableInfo, error) {
	info := &TableInfo{}

	for rows.Next() {
		var (
			cid     int
			name    string
			colType string
			notNull int
			dfltVal sql.NullString
			pk      int
		)

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltVal, &pk); err != nil {
			return nil, fmt.Errorf("schemashift/sqlite: scan pragma table_info: %w", err)
		}

		col := ColumnInfo{
			Name:     name,
			Type:     colType,
			Nullable: notNull == 0, // notnull=1 means NOT NULL, so nullable is inverse
		}

		if dfltVal.Valid {
			col.Default = &dfltVal.String
		}

		info.Columns = append(info.Columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("schemashift/sqlite: rows error: %w", err)
	}

	return info, nil
}

// RowCountSQL returns SELECT COUNT(*) for a table.
func (s *SQLiteDialect) RowCountSQL(table string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s;", QuoteIdentifier(s, table))
}

// ─── Batch Copy ───────────────────────────────────────────────────────────────

// BatchCopySQL generates INSERT OR IGNORE INTO ... SELECT ... LIMIT ... OFFSET ...
//
// SQLite requires LIMIT to be present when using OFFSET.
// INSERT OR IGNORE is SQLite's equivalent of ON CONFLICT DO NOTHING —
// it skips rows that would violate a UNIQUE or PRIMARY KEY constraint.
//
// Note: SQLite uses rowid internally for ordering, but it's not directly
// accessible in the same way as Postgres ctid. We rely on LIMIT/OFFSET
// which is stable for read-only batch copy within a single transaction.
//
// Example output:
//
//	INSERT OR IGNORE INTO "_shadow_users" ("id", "email", "created_at")
//	SELECT "id", "email", "created_at"
//	FROM "users"
//	LIMIT 1000 OFFSET 0;
func (s *SQLiteDialect) BatchCopySQL(srcTable, dstTable string, columns []string, offset, limit int) string {
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(s, col)
	}
	colList := strings.Join(quotedCols, ", ")

	return fmt.Sprintf(
		"INSERT OR IGNORE INTO %s (%s)\nSELECT %s\nFROM %s\nLIMIT %d OFFSET %d;",
		QuoteIdentifier(s, dstTable),
		colList,
		colList,
		QuoteIdentifier(s, srcTable),
		limit,
		offset,
	)
}

// ─── Placeholder ─────────────────────────────────────────────────────────────

// Placeholder returns ? for all positions — SQLite uses ? like MySQL.
func (s *SQLiteDialect) Placeholder(n int) string {
	return "?"
}

// ─── SQLite Version Helpers ───────────────────────────────────────────────────

// SQLiteVersionSQL returns the SQLite version string.
// Used by the strategy layer to warn about unsupported operations
// on older SQLite versions (e.g. DROP COLUMN before 3.35.0).
func SQLiteVersionSQL() string {
	return "SELECT sqlite_version();"
}

// ParseSQLiteVersion parses a version string like "3.39.2" into (major, minor, patch).
// Returns an error if the format is unexpected.
func ParseSQLiteVersion(version string) (major, minor, patch int, err error) {
	_, err = fmt.Sscanf(version, "%d.%d.%d", &major, &minor, &patch)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("schemashift/sqlite: failed to parse version %q: %w", version, err)
	}
	return major, minor, patch, nil
}

// SupportsDropColumn returns true if the SQLite version supports DROP COLUMN (3.35+).
func SupportsDropColumn(major, minor, patch int) bool {
	if major > 3 {
		return true
	}
	if major == 3 && minor > 35 {
		return true
	}
	if major == 3 && minor == 35 && patch >= 0 {
		return true
	}
	return false
}

// SupportsRenameColumn returns true if the SQLite version supports RENAME COLUMN (3.25+).
func SupportsRenameColumn(major, minor, patch int) bool {
	if major > 3 {
		return true
	}
	if major == 3 && minor >= 25 {
		return true
	}
	return false
}
