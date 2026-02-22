package dialect

import (
	"database/sql"
	"fmt"
	"strings"
)

// MySQLDialect implements Dialect for MySQL (and MariaDB).
type MySQLDialect struct {
	// DBName is required for information_schema queries in MySQL.
	DBName string
}

// NewMySQL returns a new MySQLDialect instance.
// dbName should be the current database name (e.g. "myapp").
func NewMySQL(dbName string) *MySQLDialect {
	return &MySQLDialect{DBName: dbName}
}

// ─── Identity ────────────────────────────────────────────────────────────────

func (m *MySQLDialect) Name() string {
	return "mysql"
}

// ─── DDL Generation ──────────────────────────────────────────────────────────

// CreateTableSQL generates CREATE TABLE IF NOT EXISTS with ENGINE=InnoDB.
// InnoDB is required for foreign keys, transactions, and row-level locking.
//
// Example output:
//
//	CREATE TABLE IF NOT EXISTS `users` (
//	    `id` INT PRIMARY KEY AUTO_INCREMENT,
//	    `email` VARCHAR(255) NOT NULL UNIQUE,
//	    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
func (m *MySQLDialect) CreateTableSQL(table string, columns []ColumnDef) string {
	var cols []string
	for _, col := range columns {
		cols = append(cols, "    "+BuildColumnDef(m, col))
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
		QuoteIdentifier(m, table),
		strings.Join(cols, ",\n"),
	)
}

// DropTableSQL generates DROP TABLE IF EXISTS.
//
// Example output:
//
//	DROP TABLE IF EXISTS `users`;
func (m *MySQLDialect) DropTableSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", QuoteIdentifier(m, table))
}

// RenameTableSQL generates RENAME TABLE ... TO ...
// MySQL uses RENAME TABLE instead of ALTER TABLE ... RENAME TO
// and it's atomic — perfect for shadow table cutover.
//
// Example output:
//
//	RENAME TABLE `users` TO `users_old`;
func (m *MySQLDialect) RenameTableSQL(oldName, newName string) string {
	return fmt.Sprintf(
		"RENAME TABLE %s TO %s;",
		QuoteIdentifier(m, oldName),
		QuoteIdentifier(m, newName),
	)
}

// AddColumnSQL generates ALTER TABLE ... ADD COLUMN ...
// MySQL does NOT support IF NOT EXISTS on ADD COLUMN (unlike Postgres),
// so callers should check column existence first via ColumnExistsSQL.
//
// Example output:
//
//	ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20);
func (m *MySQLDialect) AddColumnSQL(table string, col ColumnDef) string {
	return fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN %s;",
		QuoteIdentifier(m, table),
		BuildColumnDef(m, col),
	)
}

// DropColumnSQL generates ALTER TABLE ... DROP COLUMN ...
//
// Example output:
//
//	ALTER TABLE `users` DROP COLUMN `phone`;
func (m *MySQLDialect) DropColumnSQL(table, column string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s DROP COLUMN %s;",
		QuoteIdentifier(m, table),
		QuoteIdentifier(m, column),
	)
}

// RenameColumnSQL generates ALTER TABLE ... RENAME COLUMN ... TO ...
// Supported in MySQL 8.0+. For older versions, CHANGE COLUMN must be used
// (which requires repeating the full column definition — not supported here).
//
// Example output:
//
//	ALTER TABLE `users` RENAME COLUMN `phone` TO `mobile`;
func (m *MySQLDialect) RenameColumnSQL(table, oldCol, newCol string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s RENAME COLUMN %s TO %s;",
		QuoteIdentifier(m, table),
		QuoteIdentifier(m, oldCol),
		QuoteIdentifier(m, newCol),
	)
}

// CreateIndexSQL generates CREATE [UNIQUE] INDEX IF NOT EXISTS ...
// MySQL 8.0.29+ supports IF NOT EXISTS on indexes.
//
// Example output:
//
//	CREATE UNIQUE INDEX IF NOT EXISTS `idx_users_email` ON `users` (`email`);
func (m *MySQLDialect) CreateIndexSQL(idx IndexDef) string {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}

	quotedCols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		quotedCols[i] = QuoteIdentifier(m, col)
	}

	return fmt.Sprintf(
		"CREATE %sINDEX IF NOT EXISTS %s ON %s (%s);",
		unique,
		QuoteIdentifier(m, idx.Name),
		QuoteIdentifier(m, idx.Table),
		strings.Join(quotedCols, ", "),
	)
}

// DropIndexSQL generates DROP INDEX ... ON ... (MySQL requires the table name).
//
// Example output:
//
//	DROP INDEX `idx_users_email` ON `users`;
func (m *MySQLDialect) DropIndexSQL(table, indexName string) string {
	return fmt.Sprintf(
		"DROP INDEX %s ON %s;",
		QuoteIdentifier(m, indexName),
		QuoteIdentifier(m, table),
	)
}

// ─── Locking ─────────────────────────────────────────────────────────────────

// AcquireLockSQL uses MySQL's GET_LOCK() function.
// GET_LOCK(key, timeout) — timeout of 0 means try once and return immediately.
// Returns 1 if acquired, 0 if timeout, NULL on error.
//
// Important: MySQL GET_LOCK is connection-scoped.
// The same connection must be used to release the lock.
//
// Example output:
//
//	SELECT GET_LOCK('schemashift_myapp', 0);
func (m *MySQLDialect) AcquireLockSQL(lockKey string) (string, bool) {
	return fmt.Sprintf(
		"SELECT GET_LOCK('%s', 0);",
		escapeSingleQuote(lockKey),
	), true
}

// ReleaseLockSQL uses MySQL's RELEASE_LOCK() function.
// Returns 1 if released, 0 if not held by this connection, NULL if doesn't exist.
//
// Example output:
//
//	SELECT RELEASE_LOCK('schemashift_myapp');
func (m *MySQLDialect) ReleaseLockSQL(lockKey string) (string, bool) {
	return fmt.Sprintf(
		"SELECT RELEASE_LOCK('%s');",
		escapeSingleQuote(lockKey),
	), true
}

// ─── Introspection ───────────────────────────────────────────────────────────

// TableExistsSQL checks information_schema.tables for MySQL.
// Requires DBName to scope correctly.
//
// Example output:
//
//	SELECT COUNT(*) FROM information_schema.tables
//	WHERE table_schema = 'myapp' AND table_name = 'users';
func (m *MySQLDialect) TableExistsSQL(table string) string {
	return fmt.Sprintf(`
SELECT COUNT(*) 
FROM information_schema.tables 
WHERE table_schema = '%s' 
  AND table_name = '%s';`,
		escapeSingleQuote(m.DBName),
		escapeSingleQuote(table),
	)
}

// ColumnExistsSQL checks information_schema.columns for MySQL.
//
// Example output:
//
//	SELECT COUNT(*) FROM information_schema.columns
//	WHERE table_schema = 'myapp' AND table_name = 'users' AND column_name = 'email';
func (m *MySQLDialect) ColumnExistsSQL(table, column string) string {
	return fmt.Sprintf(`
SELECT COUNT(*) 
FROM information_schema.columns 
WHERE table_schema = '%s' 
  AND table_name = '%s' 
  AND column_name = '%s';`,
		escapeSingleQuote(m.DBName),
		escapeSingleQuote(table),
		escapeSingleQuote(column),
	)
}

// GetTableInfoSQL returns column metadata for MySQL.
// Returns: column_name, data_type, is_nullable, column_default
func (m *MySQLDialect) GetTableInfoSQL(table string) string {
	return fmt.Sprintf(`
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = '%s'
  AND table_name = '%s'
ORDER BY ordinal_position;`,
		escapeSingleQuote(m.DBName),
		escapeSingleQuote(table),
	)
}

// ParseTableInfo reads sql.Rows from GetTableInfoSQL and builds a TableInfo.
// MySQL returns is_nullable as "YES"/"NO" — same as Postgres.
func (m *MySQLDialect) ParseTableInfo(rows *sql.Rows) (*TableInfo, error) {
	info := &TableInfo{}

	for rows.Next() {
		var col ColumnInfo
		var isNullable string
		var colDefault sql.NullString

		if err := rows.Scan(&col.Name, &col.Type, &isNullable, &colDefault); err != nil {
			return nil, fmt.Errorf("schemashift/mysql: scan column info: %w", err)
		}

		col.Nullable = strings.EqualFold(isNullable, "YES")
		if colDefault.Valid {
			col.Default = &colDefault.String
		}

		info.Columns = append(info.Columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("schemashift/mysql: rows error: %w", err)
	}

	return info, nil
}

// RowCountSQL returns SELECT COUNT(*) for a table.
func (m *MySQLDialect) RowCountSQL(table string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s;", QuoteIdentifier(m, table))
}

// ─── Batch Copy ───────────────────────────────────────────────────────────────

// BatchCopySQL generates INSERT IGNORE INTO ... SELECT ... LIMIT ... OFFSET ...
// INSERT IGNORE makes it idempotent — duplicate rows are silently skipped.
//
// MySQL does not have ctid, so we use LIMIT/OFFSET directly.
// For large tables, consider adding an ORDER BY primary key for stability.
//
// Example output:
//
//	INSERT IGNORE INTO `_shadow_users` (`id`, `email`, `created_at`)
//	SELECT `id`, `email`, `created_at`
//	FROM `users`
//	LIMIT 1000 OFFSET 0;
func (m *MySQLDialect) BatchCopySQL(srcTable, dstTable string, columns []string, offset, limit int) string {
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(m, col)
	}
	colList := strings.Join(quotedCols, ", ")

	return fmt.Sprintf(
		"INSERT IGNORE INTO %s (%s)\nSELECT %s\nFROM %s\nLIMIT %d OFFSET %d;",
		QuoteIdentifier(m, dstTable),
		colList,
		colList,
		QuoteIdentifier(m, srcTable),
		limit,
		offset,
	)
}

// ─── Placeholder ─────────────────────────────────────────────────────────────

// Placeholder returns ? for all positions — MySQL uses positional ? placeholders.
func (m *MySQLDialect) Placeholder(n int) string {
	return "?"
}
