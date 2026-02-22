package migration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/SravanKolanu20/schemashift/dialect"
)

// CheckpointState represents the saved state of a migration at a point in time.
type CheckpointState string

const (
	// CheckpointStarted is saved before the first step executes.
	CheckpointStarted CheckpointState = "started"

	// CheckpointStepComplete is saved after each successful step.
	CheckpointStepComplete CheckpointState = "step_complete"

	// CheckpointComplete is saved after all steps succeed.
	CheckpointComplete CheckpointState = "complete"

	// CheckpointFailed is saved when a step fails.
	CheckpointFailed CheckpointState = "failed"

	// CheckpointRollingBack is saved when rollback begins.
	CheckpointRollingBack CheckpointState = "rolling_back"

	// CheckpointRolledBack is saved when rollback completes.
	CheckpointRolledBack CheckpointState = "rolled_back"
)

// Checkpoint represents a single saved state for a migration.
// Stored in the _schemashift_checkpoints table.
type Checkpoint struct {
	// ID is the auto-incremented primary key.
	ID int64

	// Version is the migration version this checkpoint belongs to.
	Version string

	// State is the checkpoint state at time of save.
	State CheckpointState

	// StepIndex is the index of the last completed step (0-based).
	// -1 means no steps have completed yet (just started).
	StepIndex int

	// StepKind is the kind of the last step that ran.
	StepKind string

	// Metadata holds arbitrary JSON data about the checkpoint.
	// Used to store shadow table names, batch progress, etc.
	Metadata map[string]any

	// Error holds the error message if State is CheckpointFailed.
	Error string

	// CreatedAt is when this checkpoint was saved.
	CreatedAt time.Time
}

// MetadataJSON serializes the Metadata map to JSON.
func (c *Checkpoint) MetadataJSON() (string, error) {
	if c.Metadata == nil {
		return "{}", nil
	}
	b, err := json.Marshal(c.Metadata)
	if err != nil {
		return "", fmt.Errorf("schemashift/checkpoint: marshal metadata: %w", err)
	}
	return string(b), nil
}

// ParseMetadata deserializes a JSON string into the Metadata map.
func (c *Checkpoint) ParseMetadata(raw string) error {
	if raw == "" || raw == "{}" {
		c.Metadata = make(map[string]any)
		return nil
	}
	if err := json.Unmarshal([]byte(raw), &c.Metadata); err != nil {
		return fmt.Errorf("schemashift/checkpoint: parse metadata: %w", err)
	}
	return nil
}

// Get retrieves a typed value from the Metadata map.
// Returns zero value and false if key is missing or type mismatch.
func (c *Checkpoint) Get(key string) (any, bool) {
	if c.Metadata == nil {
		return nil, false
	}
	val, ok := c.Metadata[key]
	return val, ok
}

// Set stores a value in the Metadata map.
func (c *Checkpoint) Set(key string, val any) {
	if c.Metadata == nil {
		c.Metadata = make(map[string]any)
	}
	c.Metadata[key] = val
}

// ─── CheckpointStore ──────────────────────────────────────────────────────────

// CheckpointStore manages reading and writing checkpoints to the database.
// It uses its own dedicated table: _schemashift_checkpoints.
type CheckpointStore struct {
	db      *sql.DB
	dialect dialect.Dialect
}

// NewCheckpointStore creates a new CheckpointStore.
func NewCheckpointStore(db *sql.DB, d dialect.Dialect) *CheckpointStore {
	return &CheckpointStore{db: db, dialect: d}
}

// ─── Schema Bootstrap ─────────────────────────────────────────────────────────

// EnsureTable creates the _schemashift_checkpoints table if it doesn't exist.
// Called once during Shifter initialization.
func (cs *CheckpointStore) EnsureTable(ctx context.Context) error {
	query := cs.createTableSQL()
	if _, err := cs.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift/checkpoint: create table: %w", err)
	}
	return nil
}

// createTableSQL returns the CREATE TABLE statement for checkpoints.
// Dialect-aware for type differences (SERIAL vs INTEGER AUTOINCREMENT etc).
func (cs *CheckpointStore) createTableSQL() string {
	switch cs.dialect.Name() {

	case "postgres":
		return `
CREATE TABLE IF NOT EXISTS _schemashift_checkpoints (
    id          BIGSERIAL PRIMARY KEY,
    version     TEXT        NOT NULL,
    state       TEXT        NOT NULL,
    step_index  INTEGER     NOT NULL DEFAULT -1,
    step_kind   TEXT        NOT NULL DEFAULT '',
    metadata    TEXT        NOT NULL DEFAULT '{}',
    error       TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ss_checkpoints_version
    ON _schemashift_checkpoints (version, created_at DESC);
`

	case "mysql":
		return `
CREATE TABLE IF NOT EXISTS _schemashift_checkpoints (
    id          BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    version     VARCHAR(255) NOT NULL,
    state       VARCHAR(50)  NOT NULL,
    step_index  INT          NOT NULL DEFAULT -1,
    step_kind   VARCHAR(100) NOT NULL DEFAULT '',
    metadata    TEXT         NOT NULL,
    error       TEXT         NOT NULL,
    created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX IF NOT EXISTS idx_ss_checkpoints_version
    ON _schemashift_checkpoints (version, created_at);
`

	case "sqlite":
		return `
CREATE TABLE IF NOT EXISTS _schemashift_checkpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     TEXT    NOT NULL,
    state       TEXT    NOT NULL,
    step_index  INTEGER NOT NULL DEFAULT -1,
    step_kind   TEXT    NOT NULL DEFAULT '',
    metadata    TEXT    NOT NULL DEFAULT '{}',
    error       TEXT    NOT NULL DEFAULT '',
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ss_checkpoints_version
    ON _schemashift_checkpoints (version, created_at);
`
	}

	return ""
}

// ─── Write Operations ─────────────────────────────────────────────────────────

// Save writes a new checkpoint row to the database.
// Each checkpoint is an immutable append — we never update existing rows.
// This gives a full audit trail of every state transition.
func (cs *CheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	metaJSON, err := cp.MetadataJSON()
	if err != nil {
		return err
	}

	query := cs.insertSQL()
	_, err = cs.db.ExecContext(ctx, query,
		cp.Version,
		string(cp.State),
		cp.StepIndex,
		cp.StepKind,
		metaJSON,
		cp.Error,
	)
	if err != nil {
		return fmt.Errorf(
			"schemashift/checkpoint: save %q state=%s step=%d: %w",
			cp.Version, cp.State, cp.StepIndex, err,
		)
	}
	return nil
}

// insertSQL returns the dialect-specific INSERT for a checkpoint row.
func (cs *CheckpointStore) insertSQL() string {
	switch cs.dialect.Name() {
	case "postgres":
		return `
INSERT INTO _schemashift_checkpoints 
    (version, state, step_index, step_kind, metadata, error)
VALUES 
    ($1, $2, $3, $4, $5, $6);`

	default: // mysql, sqlite
		return `
INSERT INTO _schemashift_checkpoints 
    (version, state, step_index, step_kind, metadata, error)
VALUES 
    (?, ?, ?, ?, ?, ?);`
	}
}

// SaveStepComplete saves a CheckpointStepComplete record after a step succeeds.
// Convenience wrapper around Save().
func (cs *CheckpointStore) SaveStepComplete(
	ctx context.Context,
	version string,
	stepIndex int,
	stepKind StepKind,
	meta map[string]any,
) error {
	return cs.Save(ctx, &Checkpoint{
		Version:   version,
		State:     CheckpointStepComplete,
		StepIndex: stepIndex,
		StepKind:  string(stepKind),
		Metadata:  meta,
	})
}

// SaveFailed saves a CheckpointFailed record when a step fails.
func (cs *CheckpointStore) SaveFailed(
	ctx context.Context,
	version string,
	stepIndex int,
	stepKind StepKind,
	execErr error,
) error {
	errMsg := ""
	if execErr != nil {
		errMsg = execErr.Error()
	}
	return cs.Save(ctx, &Checkpoint{
		Version:   version,
		State:     CheckpointFailed,
		StepIndex: stepIndex,
		StepKind:  string(stepKind),
		Error:     errMsg,
	})
}

// ─── Read Operations ──────────────────────────────────────────────────────────

// Latest returns the most recent checkpoint for a given migration version.
// Returns (nil, nil) if no checkpoint exists for that version.
func (cs *CheckpointStore) Latest(ctx context.Context, version string) (*Checkpoint, error) {
	query := cs.latestSQL()

	row := cs.db.QueryRowContext(ctx, query, version)

	cp := &Checkpoint{}
	var metaRaw string
	var createdAt string

	err := row.Scan(
		&cp.ID,
		&cp.Version,
		&cp.State,
		&cp.StepIndex,
		&cp.StepKind,
		&metaRaw,
		&cp.Error,
		&createdAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil // No checkpoint found — migration hasn't started
	}
	if err != nil {
		return nil, fmt.Errorf("schemashift/checkpoint: latest for %q: %w", version, err)
	}

	if err := cp.ParseMetadata(metaRaw); err != nil {
		return nil, err
	}

	cp.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("schemashift/checkpoint: parse created_at: %w", err)
	}

	return cp, nil
}

// latestSQL returns the dialect-specific query for the most recent checkpoint.
func (cs *CheckpointStore) latestSQL() string {
	switch cs.dialect.Name() {
	case "postgres":
		return `
SELECT id, version, state, step_index, step_kind, metadata, error, created_at
FROM _schemashift_checkpoints
WHERE version = $1
ORDER BY id DESC
LIMIT 1;`

	default: // mysql, sqlite
		return `
SELECT id, version, state, step_index, step_kind, metadata, error, created_at
FROM _schemashift_checkpoints
WHERE version = ?
ORDER BY id DESC
LIMIT 1;`
	}
}

// History returns all checkpoints for a given version in ascending order.
// Useful for debugging and audit trails.
func (cs *CheckpointStore) History(ctx context.Context, version string) ([]*Checkpoint, error) {
	query := cs.historySQL()

	rows, err := cs.db.QueryContext(ctx, query, version)
	if err != nil {
		return nil, fmt.Errorf("schemashift/checkpoint: history for %q: %w", version, err)
	}
	defer rows.Close()

	var checkpoints []*Checkpoint

	for rows.Next() {
		cp := &Checkpoint{}
		var metaRaw string
		var createdAt string

		if err := rows.Scan(
			&cp.ID,
			&cp.Version,
			&cp.State,
			&cp.StepIndex,
			&cp.StepKind,
			&metaRaw,
			&cp.Error,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("schemashift/checkpoint: scan history row: %w", err)
		}

		if err := cp.ParseMetadata(metaRaw); err != nil {
			return nil, err
		}

		cp.CreatedAt, _ = parseTime(createdAt)
		checkpoints = append(checkpoints, cp)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("schemashift/checkpoint: history rows error: %w", err)
	}

	return checkpoints, nil
}

// historySQL returns all checkpoints for a version in ascending order.
func (cs *CheckpointStore) historySQL() string {
	switch cs.dialect.Name() {
	case "postgres":
		return `
SELECT id, version, state, step_index, step_kind, metadata, error, created_at
FROM _schemashift_checkpoints
WHERE version = $1
ORDER BY id ASC;`

	default:
		return `
SELECT id, version, state, step_index, step_kind, metadata, error, created_at
FROM _schemashift_checkpoints
WHERE version = ?
ORDER BY id ASC;`
	}
}

// AllIncomplete returns all migration versions that have a "started"
// or "failed" checkpoint but no "complete" checkpoint.
// These are migrations that need attention — either resume or rollback.
func (cs *CheckpointStore) AllIncomplete(ctx context.Context) ([]string, error) {
	query := cs.incompleteSQL()

	rows, err := cs.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("schemashift/checkpoint: query incomplete: %w", err)
	}
	defer rows.Close()

	var versions []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("schemashift/checkpoint: scan incomplete row: %w", err)
		}
		versions = append(versions, version)
	}

	return versions, rows.Err()
}

// incompleteSQL finds versions with failed/started checkpoints
// that never reached the complete state.
func (cs *CheckpointStore) incompleteSQL() string {
	// Subquery finds versions that have reached complete or rolled_back.
	// Outer query finds started/failed that are NOT in that set.
	return `
SELECT DISTINCT version
FROM _schemashift_checkpoints
WHERE state IN ('started', 'failed', 'rolling_back')
  AND version NOT IN (
      SELECT DISTINCT version
      FROM _schemashift_checkpoints
      WHERE state IN ('complete', 'rolled_back')
  )
ORDER BY version;`
}

// ─── Cleanup ──────────────────────────────────────────────────────────────────

// Prune deletes checkpoints older than the given duration for completed migrations.
// Keeps the most recent checkpoint for each version regardless of age.
// Safe to call periodically to prevent the checkpoint table from growing indefinitely.
func (cs *CheckpointStore) Prune(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	query, arg := cs.pruneSQL(cutoff)

	result, err := cs.db.ExecContext(ctx, query, arg)
	if err != nil {
		return 0, fmt.Errorf("schemashift/checkpoint: prune: %w", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, nil // Non-fatal — some drivers don't support RowsAffected
	}
	return deleted, nil
}

// pruneSQL returns dialect-specific DELETE for old checkpoints.
// Preserves at least the latest checkpoint per version.
func (cs *CheckpointStore) pruneSQL(cutoff time.Time) (string, any) {
	cutoffStr := cutoff.UTC().Format(time.RFC3339)

	switch cs.dialect.Name() {
	case "postgres":
		return `
DELETE FROM _schemashift_checkpoints
WHERE created_at < $1
  AND state IN ('complete', 'rolled_back')
  AND id NOT IN (
      SELECT MAX(id)
      FROM _schemashift_checkpoints
      GROUP BY version
  );`, cutoffStr

	default: // mysql, sqlite
		return `
DELETE FROM _schemashift_checkpoints
WHERE created_at < ?
  AND state IN ('complete', 'rolled_back')
  AND id NOT IN (
      SELECT max_id FROM (
          SELECT MAX(id) AS max_id
          FROM _schemashift_checkpoints
          GROUP BY version
      ) AS latest
  );`, cutoffStr
	}
}

// ─── Recovery Helpers ─────────────────────────────────────────────────────────

// RecoveryAction describes what the Shifter should do with
// an incomplete migration found on startup.
type RecoveryAction string

const (
	// RecoveryRollback means the migration should be reversed.
	RecoveryRollback RecoveryAction = "rollback"

	// RecoveryResume means the migration can be resumed from the last good step.
	RecoveryResume RecoveryAction = "resume"

	// RecoveryManual means human intervention is required.
	RecoveryManual RecoveryAction = "manual"
)

// RecommendRecovery examines the latest checkpoint for a migration
// and recommends a recovery action.
func RecommendRecovery(cp *Checkpoint) RecoveryAction {
	if cp == nil {
		return RecoveryManual
	}

	switch cp.State {

	case CheckpointStarted:
		// Migration started but no steps completed — safe to rollback cleanly
		return RecoveryRollback

	case CheckpointStepComplete:
		// Some steps completed — we can try to resume from last good step
		// OR rollback. Default to rollback for safety.
		return RecoveryRollback

	case CheckpointFailed:
		// Migration explicitly failed — rollback is the right move
		return RecoveryRollback

	case CheckpointRollingBack:
		// Was mid-rollback when process died — resume the rollback
		return RecoveryRollback

	case CheckpointComplete:
		// Should not be in incomplete list — manual investigation
		return RecoveryManual

	case CheckpointRolledBack:
		// Already rolled back — manual investigation needed
		return RecoveryManual
	}

	return RecoveryManual
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

// parseTime parses a time string from any of the formats
// used by Postgres, MySQL, and SQLite.
func parseTime(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, layout := range formats {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("schemashift/checkpoint: cannot parse time %q", s)
}
