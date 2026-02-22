package tracker

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sravankolanu20/schemashift/dialect"
)

// schemaManager handles creation and validation of the
// _schemashift_migrations table — the permanent migration history.
type schemaManager struct {
	db      *sql.DB
	dialect dialect.Dialect
}

// newSchemaManager creates a new schemaManager.
func newSchemaManager(db *sql.DB, d dialect.Dialect) *schemaManager {
	return &schemaManager{db: db, dialect: d}
}

// EnsureTable creates the _schemashift_migrations table if it doesn't exist.
// This is idempotent — safe to call on every application startup.
//
// Table structure:
//
//	id           — auto-incremented primary key
//	version      — unique migration version string
//	description  — human-readable description
//	applied_at   — when the migration completed successfully
//	execution_ms — how long the migration took in milliseconds
//	checksum     — hash of the migration steps for drift detection
//	dialect      — which DB dialect was used
//	steps_count  — number of steps in the migration
func (s *schemaManager) EnsureTable(ctx context.Context) error {
	queries := s.bootstrapSQL()
	for _, query := range queries {
		if query == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf(
				"schemashift/tracker: bootstrap schema: %w", err,
			)
		}
	}
	return nil
}

// bootstrapSQL returns the ordered list of SQL statements needed
// to create the migrations tracking table and its indexes.
func (s *schemaManager) bootstrapSQL() []string {
	switch s.dialect.Name() {

	case "postgres":
		return []string{
			`CREATE TABLE IF NOT EXISTS _schemashift_migrations (
    id             BIGSERIAL    PRIMARY KEY,
    version        TEXT         NOT NULL UNIQUE,
    description    TEXT         NOT NULL DEFAULT '',
    applied_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    execution_ms   BIGINT       NOT NULL DEFAULT 0,
    checksum       TEXT         NOT NULL DEFAULT '',
    dialect        TEXT         NOT NULL DEFAULT '',
    steps_count    INTEGER      NOT NULL DEFAULT 0
);`,
			`CREATE INDEX IF NOT EXISTS idx_ss_migrations_version
    ON _schemashift_migrations (version);`,
			`CREATE INDEX IF NOT EXISTS idx_ss_migrations_applied_at
    ON _schemashift_migrations (applied_at DESC);`,
		}

	case "mysql":
		return []string{
			`CREATE TABLE IF NOT EXISTS _schemashift_migrations (
    id             BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    version        VARCHAR(255) NOT NULL,
    description    TEXT         NOT NULL,
    applied_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    execution_ms   BIGINT       NOT NULL DEFAULT 0,
    checksum       VARCHAR(64)  NOT NULL DEFAULT '',
    dialect        VARCHAR(20)  NOT NULL DEFAULT '',
    steps_count    INT          NOT NULL DEFAULT 0,
    CONSTRAINT uq_ss_migrations_version UNIQUE (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`,
			`CREATE INDEX IF NOT EXISTS idx_ss_migrations_applied_at
    ON _schemashift_migrations (applied_at);`,
		}

	case "sqlite":
		return []string{
			`CREATE TABLE IF NOT EXISTS _schemashift_migrations (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    version        TEXT    NOT NULL UNIQUE,
    description    TEXT    NOT NULL DEFAULT '',
    applied_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    execution_ms   INTEGER NOT NULL DEFAULT 0,
    checksum       TEXT    NOT NULL DEFAULT '',
    dialect        TEXT    NOT NULL DEFAULT '',
    steps_count    INTEGER NOT NULL DEFAULT 0
);`,
			`CREATE INDEX IF NOT EXISTS idx_ss_migrations_version
    ON _schemashift_migrations (version);`,
		}
	}

	return nil
}

// DropTable drops the _schemashift_migrations table.
// DANGER: This erases all migration history permanently.
// Only used in tests or when completely resetting a schema.
func (s *schemaManager) DropTable(ctx context.Context) error {
	query := "DROP TABLE IF EXISTS _schemashift_migrations;"
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift/tracker: drop table: %w", err)
	}
	return nil
}

// TableExists checks whether the _schemashift_migrations table exists.
func (s *schemaManager) TableExists(ctx context.Context) (bool, error) {
	query := s.dialect.TableExistsSQL("_schemashift_migrations")
	var count int
	if err := s.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("schemashift/tracker: check table exists: %w", err)
	}
	return count > 0, nil
}

// ValidateSchema checks that the existing _schemashift_migrations table
// has the expected columns. Returns an error if the schema is out of date
// or corrupted. Used during Shifter initialization.
func (s *schemaManager) ValidateSchema(ctx context.Context) error {
	requiredColumns := []string{
		"version",
		"description",
		"applied_at",
		"execution_ms",
		"checksum",
		"dialect",
		"steps_count",
	}

	for _, col := range requiredColumns {
		query := s.dialect.ColumnExistsSQL("_schemashift_migrations", col)
		var count int
		if err := s.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf(
				"schemashift/tracker: validate column %q: %w", col, err,
			)
		}
		if count == 0 {
			return fmt.Errorf(
				"schemashift/tracker: _schemashift_migrations is missing column %q — the table may be from an older version",
				col,
			)
		}
	}

	return nil
}
