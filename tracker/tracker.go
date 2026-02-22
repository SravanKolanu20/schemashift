package tracker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SravanKolanu20/schemashift/dialect"
	"github.com/SravanKolanu20/schemashift/migration"
)

// Record is a single row in the _schemashift_migrations table.
// Represents a permanently applied migration.
type Record struct {
	// ID is the auto-incremented primary key.
	ID int64

	// Version is the migration version string.
	Version string

	// Description is the human-readable description.
	Description string

	// AppliedAt is when the migration was successfully applied.
	AppliedAt time.Time

	// ExecutionMs is how long the migration took in milliseconds.
	ExecutionMs int64

	// Checksum is the SHA-256 hash of the migration's step kinds + version.
	// Used to detect if a migration's definition has changed after being applied.
	Checksum string

	// Dialect is the DB dialect used when this migration was applied.
	Dialect string

	// StepsCount is the number of steps in the migration.
	StepsCount int
}

// ─── Tracker ──────────────────────────────────────────────────────────────────

// Tracker manages the permanent migration history table.
// It records which migrations have been applied and provides
// the applied version set to the Planner.
type Tracker struct {
	db      *sql.DB
	dialect dialect.Dialect
	schema  *schemaManager
}

// New creates a new Tracker.
func New(db *sql.DB, d dialect.Dialect) *Tracker {
	return &Tracker{
		db:      db,
		dialect: d,
		schema:  newSchemaManager(db, d),
	}
}

// ─── Initialization ───────────────────────────────────────────────────────────

// Init ensures the tracking table exists and its schema is valid.
// Called once during Shifter startup before any migrations run.
func (t *Tracker) Init(ctx context.Context) error {
	// Check if table already exists
	exists, err := t.schema.TableExists(ctx)
	if err != nil {
		return err
	}

	if !exists {
		// First time — create the table from scratch
		if err := t.schema.EnsureTable(ctx); err != nil {
			return fmt.Errorf("schemashift/tracker: init: %w", err)
		}
		return nil
	}

	// Table exists — validate its schema matches what we expect
	if err := t.schema.ValidateSchema(ctx); err != nil {
		return fmt.Errorf("schemashift/tracker: schema validation failed: %w", err)
	}

	return nil
}

// ─── Write Operations ─────────────────────────────────────────────────────────

// MarkApplied records a migration as successfully applied.
// Called after all steps complete and the migration commits.
func (t *Tracker) MarkApplied(ctx context.Context, m *migration.Migration) error {
	checksum := ComputeChecksum(m)
	execMs := m.ExecutionTime.Milliseconds()

	query, args := t.insertSQL(
		m.Version,
		m.Description,
		execMs,
		checksum,
		t.dialect.Name(),
		len(m.Steps),
	)

	if _, err := t.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf(
			"schemashift/tracker: mark applied %q: %w", m.Version, err,
		)
	}
	return nil
}

// insertSQL returns the dialect-specific INSERT for a migration record.
func (t *Tracker) insertSQL(
	version, description string,
	execMs int64,
	checksum, dialectName string,
	stepsCount int,
) (string, []any) {
	args := []any{
		version,
		description,
		execMs,
		checksum,
		dialectName,
		stepsCount,
	}

	switch t.dialect.Name() {
	case "postgres":
		return `
INSERT INTO _schemashift_migrations
    (version, description, execution_ms, checksum, dialect, steps_count)
VALUES
    ($1, $2, $3, $4, $5, $6)
ON CONFLICT (version) DO NOTHING;`, args

	default: // mysql, sqlite
		return `
INSERT OR IGNORE INTO _schemashift_migrations
    (version, description, execution_ms, checksum, dialect, steps_count)
VALUES
    (?, ?, ?, ?, ?, ?);`, args
	}
}

// MarkRolledBack removes the migration record from the tracker.
// Called after a successful rollback — the migration is now pending again.
func (t *Tracker) MarkRolledBack(ctx context.Context, version string) error {
	query, arg := t.deleteSQL(version)
	if _, err := t.db.ExecContext(ctx, query, arg); err != nil {
		return fmt.Errorf(
			"schemashift/tracker: mark rolled back %q: %w", version, err,
		)
	}
	return nil
}

// deleteSQL returns the dialect-specific DELETE for removing a migration record.
func (t *Tracker) deleteSQL(version string) (string, any) {
	switch t.dialect.Name() {
	case "postgres":
		return `DELETE FROM _schemashift_migrations WHERE version = $1;`, version
	default:
		return `DELETE FROM _schemashift_migrations WHERE version = ?;`, version
	}
}

// ─── Read Operations ──────────────────────────────────────────────────────────

// AppliedVersions returns a set of all applied migration version strings.
// This is the primary input to the Planner — O(1) lookup per migration.
func (t *Tracker) AppliedVersions(ctx context.Context) (map[string]bool, error) {
	rows, err := t.db.QueryContext(ctx,
		"SELECT version FROM _schemashift_migrations ORDER BY applied_at ASC;",
	)
	if err != nil {
		return nil, fmt.Errorf("schemashift/tracker: query applied versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("schemashift/tracker: scan version: %w", err)
		}
		applied[version] = true
	}

	return applied, rows.Err()
}

// All returns all migration records ordered by applied_at ascending.
// Used for status reporting and audit.
func (t *Tracker) All(ctx context.Context) ([]*Record, error) {
	rows, err := t.db.QueryContext(ctx, t.selectAllSQL())
	if err != nil {
		return nil, fmt.Errorf("schemashift/tracker: query all records: %w", err)
	}
	defer rows.Close()

	var records []*Record

	for rows.Next() {
		r, err := t.scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}

	return records, rows.Err()
}

// selectAllSQL returns the SELECT for all migration records.
func (t *Tracker) selectAllSQL() string {
	return `
SELECT 
    id, version, description, applied_at,
    execution_ms, checksum, dialect, steps_count
FROM _schemashift_migrations
ORDER BY applied_at ASC;`
}

// Get returns the record for a specific migration version.
// Returns (nil, nil) if the version has not been applied.
func (t *Tracker) Get(ctx context.Context, version string) (*Record, error) {
	query, arg := t.selectOneSQL(version)
	row := t.db.QueryRowContext(ctx, query, arg)

	r := &Record{}
	var appliedAt string

	err := row.Scan(
		&r.ID,
		&r.Version,
		&r.Description,
		&appliedAt,
		&r.ExecutionMs,
		&r.Checksum,
		&r.Dialect,
		&r.StepsCount,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("schemashift/tracker: get %q: %w", version, err)
	}

	r.AppliedAt, err = parseTrackerTime(appliedAt)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// selectOneSQL returns the SELECT for a single migration record.
func (t *Tracker) selectOneSQL(version string) (string, any) {
	switch t.dialect.Name() {
	case "postgres":
		return `
SELECT id, version, description, applied_at,
       execution_ms, checksum, dialect, steps_count
FROM _schemashift_migrations
WHERE version = $1;`, version
	default:
		return `
SELECT id, version, description, applied_at,
       execution_ms, checksum, dialect, steps_count
FROM _schemashift_migrations
WHERE version = ?;`, version
	}
}

// scanRecord reads a single Record from sql.Rows.
func (t *Tracker) scanRecord(rows *sql.Rows) (*Record, error) {
	r := &Record{}
	var appliedAt string

	if err := rows.Scan(
		&r.ID,
		&r.Version,
		&r.Description,
		&appliedAt,
		&r.ExecutionMs,
		&r.Checksum,
		&r.Dialect,
		&r.StepsCount,
	); err != nil {
		return nil, fmt.Errorf("schemashift/tracker: scan record: %w", err)
	}

	var err error
	r.AppliedAt, err = parseTrackerTime(appliedAt)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// ─── Drift Detection ──────────────────────────────────────────────────────────

// CheckDrift compares the checksum of each registered migration against
// the stored checksum in the tracker. Returns a list of DriftReport entries
// for any migration whose definition has changed since it was applied.
//
// Drift means a migration was applied but then someone modified its steps —
// this is dangerous because the live schema may not match what the code expects.
func (t *Tracker) CheckDrift(
	ctx context.Context,
	migrations []*migration.Migration,
) ([]*DriftReport, error) {

	var reports []*DriftReport

	for _, m := range migrations {
		record, err := t.Get(ctx, m.Version)
		if err != nil {
			return nil, err
		}
		if record == nil {
			// Not yet applied — no drift possible
			continue
		}

		currentChecksum := ComputeChecksum(m)
		if currentChecksum != record.Checksum {
			reports = append(reports, &DriftReport{
				Version:          m.Version,
				StoredChecksum:   record.Checksum,
				CurrentChecksum:  currentChecksum,
				AppliedAt:        record.AppliedAt,
				AppliedDialect:   record.Dialect,
				CurrentSteps:     len(m.Steps),
				StoredStepsCount: record.StepsCount,
			})
		}
	}

	return reports, nil
}

// DriftReport describes a migration whose definition has changed
// after being applied to the database.
type DriftReport struct {
	Version          string
	StoredChecksum   string
	CurrentChecksum  string
	AppliedAt        time.Time
	AppliedDialect   string
	CurrentSteps     int
	StoredStepsCount int
}

// String returns a human-readable drift report.
func (d *DriftReport) String() string {
	return fmt.Sprintf(
		"DRIFT: migration %q was applied on %s with %d steps (checksum: %s) "+
			"but now has %d steps (checksum: %s)",
		d.Version,
		d.AppliedAt.Format(time.RFC3339),
		d.StoredStepsCount,
		d.StoredChecksum[:8],
		d.CurrentSteps,
		d.CurrentChecksum[:8],
	)
}

// ─── Status Reporting ─────────────────────────────────────────────────────────

// Status returns a formatted status table of all migrations —
// applied ones from the tracker and pending ones from the registered list.
func (t *Tracker) Status(
	ctx context.Context,
	registered []*migration.Migration,
) (string, error) {

	applied, err := t.AppliedVersions(ctx)
	if err != nil {
		return "", err
	}

	records, err := t.All(ctx)
	if err != nil {
		return "", err
	}

	// Index records by version for O(1) lookup
	recordMap := make(map[string]*Record, len(records))
	for _, r := range records {
		recordMap[r.Version] = r
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%-40s %-12s %-25s %s\n",
		"VERSION", "STATE", "APPLIED AT", "DESCRIPTION",
	)
	fmt.Fprintf(&sb, "%s\n", strings.Repeat("─", 100))

	for _, m := range registered {
		if applied[m.Version] {
			r := recordMap[m.Version]
			appliedAt := "unknown"
			if r != nil {
				appliedAt = r.AppliedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Fprintf(&sb, "%-40s %-12s %-25s %s\n",
				m.Version, "✓ applied", appliedAt, m.Description,
			)
		} else {
			fmt.Fprintf(&sb, "%-40s %-12s %-25s %s\n",
				m.Version, "○ pending", "—", m.Description,
			)
		}
	}

	fmt.Fprintf(&sb, "\n  applied: %d  pending: %d  total: %d\n",
		len(applied),
		len(registered)-len(applied),
		len(registered),
	)

	return sb.String(), nil
}

// ─── Checksum ─────────────────────────────────────────────────────────────────

// ComputeChecksum computes a deterministic SHA-256 checksum for a migration.
// The checksum is based on the version string and the ordered list of step kinds.
// This lets us detect if a migration's definition has changed after being applied.
func ComputeChecksum(m *migration.Migration) string {
	parts := make([]string, 0, len(m.Steps)+1)
	parts = append(parts, m.Version)

	// Collect step kinds in order
	for _, step := range m.Steps {
		parts = append(parts, string(step.Kind()))
	}

	// Sort for stability — order of step kinds shouldn't change checksum
	// if you just reorder identical steps (debatable, but pragmatic)
	sorted := make([]string, len(parts))
	copy(sorted, parts)
	sort.Strings(sorted[1:]) // Sort everything except the version prefix

	raw := strings.Join(sorted, "|")
	sum := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", sum)
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

// parseTrackerTime parses timestamp strings from the migrations table.
// Handles format differences between Postgres, MySQL, and SQLite.
func parseTrackerTime(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, layout := range formats {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf(
		"schemashift/tracker: cannot parse time %q", s,
	)
}
