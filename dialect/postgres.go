package dialect

import (
	"database/sql"
	"fmt"
	"strings"
)

// PostgresDialect implements Dialect for PostgreSQL.
type PostgresDialect struct{}

// NewPostgres returns a new PostgresDialect instance.
func NewPostgres() *PostgresDialect {
	return &PostgresDialect{}
}

// ─── Identity ────────────────────────────────────────────────────────────────

func (p *PostgresDialect) Name() string {
	return "postgres"
}

// ─── DDL Generation ──────────────────────────────────────────────────────────

// CreateTableSQL generates a CREATE TABLE IF NOT EXISTS with all column definitions.
//
// Example output:
//
//	CREATE TABLE IF NOT EXISTS "users" (
//	    "id" SERIAL PRIMARY KEY,
//	    "email" TEXT NOT NULL UNIQUE,
//	    "created_at" TIMESTAMP DEFAULT now()
//	);
func (p *PostgresDialect) CreateTableSQL(table string, columns []ColumnDef) string {
	var cols []string
	for _, col := range columns {
		cols = append(cols, "    "+BuildColumnDef(p, col))
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n%s\n);",
		QuoteIdentifier(p, table),
		strings.Join(cols, ",\n"),
	)
}

// DropTableSQL generates DROP TABLE IF EXISTS.
//
// Example output:
//
//	DROP TABLE IF EXISTS "users";
func (p *PostgresDialect) DropTableSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s;", QuoteIdentifier(p, table))
}

// RenameTableSQL generates ALTER TABLE ... RENAME TO ...
//
// Example output:
//
//	ALTER TABLE "users" RENAME TO "users_old";
func (p *PostgresDialect) RenameTableSQL(oldName, newName string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s RENAME TO %s;",
		QuoteIdentifier(p, oldName),
		QuoteIdentifier(p, newName),
	)
}

// AddColumnSQL generates ALTER TABLE ... ADD COLUMN ...
//
// Example output:
//
//	ALTER TABLE "users" ADD COLUMN "phone" TEXT;
func (p *PostgresDialect) AddColumnSQL(table string, col ColumnDef) string {
	return fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s;",
		QuoteIdentifier(p, table),
		BuildColumnDef(p, col),
	)
}

// DropColumnSQL generates ALTER TABLE ... DROP COLUMN ...
//
// Example output:
//
//	ALTER TABLE "users" DROP COLUMN IF EXISTS "phone";
func (p *PostgresDialect) DropColumnSQL(table, column string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s DROP COLUMN IF EXISTS %s;",
		QuoteIdentifier(p, table),
		QuoteIdentifier(p, column),
	)
}

// RenameColumnSQL generates ALTER TABLE ... RENAME COLUMN ... TO ...
//
// Example output:
//
//	ALTER TABLE "users" RENAME COLUMN "phone" TO "mobile";
func (p *PostgresDialect) RenameColumnSQL(table, oldCol, newCol string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s RENAME COLUMN %s TO %s;",
		QuoteIdentifier(p, table),
		QuoteIdentifier(p, oldCol),
		QuoteIdentifier(p, newCol),
	)
}

// CreateIndexSQL generates CREATE [UNIQUE] INDEX IF NOT EXISTS ...
//
// Example output:
//
//	CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email");
func (p *PostgresDialect) CreateIndexSQL(idx IndexDef) string {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}

	// Quote each column name
	quotedCols := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		quotedCols[i] = QuoteIdentifier(p, col)
	}

	return fmt.Sprintf(
		"CREATE %sINDEX IF NOT EXISTS %s ON %s (%s);",
		unique,
		QuoteIdentifier(p, idx.Name),
		QuoteIdentifier(p, idx.Table),
		strings.Join(quotedCols, ", "),
	)
}

// DropIndexSQL generates DROP INDEX IF EXISTS ...
// Note: In Postgres, indexes are not table-scoped so table param is unused.
//
// Example output:
//
//	DROP INDEX IF EXISTS "idx_users_email";
func (p *PostgresDialect) DropIndexSQL(table, indexName string) string {
	return fmt.Sprintf("DROP INDEX IF EXISTS %s;", QuoteIdentifier(p, indexName))
}

// ─── Locking ─────────────────────────────────────────────────────────────────

// AcquireLockSQL uses Postgres advisory locks via pg_try_advisory_lock.
// The lockKey string is hashed into a bigint using hashtext().
//
// Returns true immediately if lock acquired, false if already locked.
//
// Example output:
//
//	SELECT pg_try_advisory_lock(hashtext('schemashift_myapp'));
func (p *PostgresDialect) AcquireLockSQL(lockKey string) (string, bool) {
	return fmt.Sprintf(
		"SELECT pg_try_advisory_lock(hashtext('%s'));",
		escapeSingleQuote(lockKey),
	), true
}

// ReleaseLockSQL releases the advisory lock acquired by AcquireLockSQL.
//
// Example output:
//
//	SELECT pg_advisory_unlock(hashtext('schemashift_myapp'));
func (p *PostgresDialect) ReleaseLockSQL(lockKey string) (string, bool) {
	return fmt.Sprintf(
		"SELECT pg_advisory_unlock(hashtext('%s'));",
		escapeSingleQuote(lockKey),
	), true
}

// ─── Introspection ───────────────────────────────────────────────────────────

// TableExistsSQL queries information_schema to check table existence.
//
// Example output:
//
//	SELECT COUNT(*) FROM information_schema.tables
//	WHERE table_schema = 'public' AND table_name = 'users';
func (p *PostgresDialect) TableExistsSQL(table string) string {
	return fmt.Sprintf(`
SELECT COUNT(*) 
FROM information_schema.tables 
WHERE table_schema = 'public' 
  AND table_name = '%s';`,
		escapeSingleQuote(table),
	)
}

// ColumnExistsSQL queries information_schema to check column existence.
//
// Example output:
//
//	SELECT COUNT(*) FROM information_schema.columns
//	WHERE table_name = 'users' AND column_name = 'email';
func (p *PostgresDialect) ColumnExistsSQL(table, column string) string {
	return fmt.Sprintf(`
SELECT COUNT(*) 
FROM information_schema.columns 
WHERE table_schema = 'public' 
  AND table_name = '%s' 
  AND column_name = '%s';`,
		escapeSingleQuote(table),
		escapeSingleQuote(column),
	)
}

// GetTableInfoSQL returns a query to fetch all column metadata for a table.
// Returns: column_name, data_type, is_nullable, column_default
func (p *PostgresDialect) GetTableInfoSQL(table string) string {
	return fmt.Sprintf(`
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = '%s'
ORDER BY ordinal_position;`,
		escapeSingleQuote(table),
	)
}

// ParseTableInfo reads sql.Rows from GetTableInfoSQL and builds a TableInfo.
func (p *PostgresDialect) ParseTableInfo(rows *sql.Rows) (*TableInfo, error) {
	info := &TableInfo{}

	for rows.Next() {
		var col ColumnInfo
		var isNullable string
		var colDefault sql.NullString

		if err := rows.Scan(&col.Name, &col.Type, &isNullable, &colDefault); err != nil {
			return nil, fmt.Errorf("schemashift/postgres: scan column info: %w", err)
		}

		col.Nullable = strings.EqualFold(isNullable, "YES")
		if colDefault.Valid {
			col.Default = &colDefault.String
		}

		info.Columns = append(info.Columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("schemashift/postgres: rows error: %w", err)
	}

	return info, nil
}

// RowCountSQL returns SELECT COUNT(*) for a table.
func (p *PostgresDialect) RowCountSQL(table string) string {
	return fmt.Sprintf(`SELECT COUNT(*) FROM %s;`, QuoteIdentifier(p, table))
}

// ─── Batch Copy ───────────────────────────────────────────────────────────────

// BatchCopySQL generates an INSERT INTO ... SELECT ... with OFFSET/LIMIT
// to copy a batch of rows from srcTable to dstTable.
//
// Example output:
//
//	INSERT INTO "_shadow_users" ("id", "email", "created_at")
//	SELECT "id", "email", "created_at"
//	FROM "users"
//	ORDER BY ctid
//	LIMIT 1000 OFFSET 0
//	ON CONFLICT DO NOTHING;
func (p *PostgresDialect) BatchCopySQL(srcTable, dstTable string, columns []string, offset, limit int) string {
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = QuoteIdentifier(p, col)
	}
	colList := strings.Join(quotedCols, ", ")

	return fmt.Sprintf(
		`INSERT INTO %s (%s)
SELECT %s
FROM %s
ORDER BY ctid
LIMIT %d OFFSET %d
ON CONFLICT DO NOTHING;`,
		QuoteIdentifier(p, dstTable),
		colList,
		colList,
		QuoteIdentifier(p, srcTable),
		limit,
		offset,
	)
}

// ─── Placeholder ─────────────────────────────────────────────────────────────

// Placeholder returns $1, $2, $3 ... style placeholders for Postgres.
func (p *PostgresDialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

// escapeSingleQuote prevents basic SQL injection in string literals.
// For production, always use parameterized queries where possible.
func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
