package lock

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/sravankolanu20/schemashift/dialect"
)

// LockResult represents the outcome of a lock acquisition attempt.
type LockResult string

const (
	// LockAcquired means we successfully hold the lock.
	LockAcquired LockResult = "acquired"

	// LockBusy means another process holds the lock.
	LockBusy LockResult = "busy"

	// LockError means something went wrong trying to acquire the lock.
	LockError LockResult = "error"
)

// Lock represents a held distributed lock.
// Always defer lock.Release() after acquiring.
type Lock struct {
	key     string
	dialect dialect.Dialect
	db      *sql.DB

	// conn is a pinned connection — required for MySQL GET_LOCK()
	// which is connection-scoped. Nil for Postgres and SQLite.
	conn *sql.Conn

	// acquired tracks whether this lock is currently held.
	acquired bool

	// acquiredAt is when the lock was acquired.
	acquiredAt time.Time
}

// Manager handles acquiring and releasing distributed locks.
// It chooses the correct locking mechanism per dialect:
//
//   - Postgres  → pg_try_advisory_lock (session-scoped)
//   - MySQL     → GET_LOCK() (connection-scoped, requires pinned conn)
//   - SQLite    → _schemashift_lock table (INSERT OR FAIL strategy)
type Manager struct {
	db      *sql.DB
	dialect dialect.Dialect

	// retryInterval is how long to wait between lock acquisition attempts.
	retryInterval time.Duration

	// maxRetries is the maximum number of acquisition attempts before giving up.
	maxRetries int
}

// NewManager creates a new lock Manager.
func NewManager(db *sql.DB, d dialect.Dialect) *Manager {
	return &Manager{
		db:            db,
		dialect:       d,
		retryInterval: 2 * time.Second,
		maxRetries:    30, // Wait up to ~60 seconds by default
	}
}

// WithRetryInterval sets how long to wait between lock attempts.
func (m *Manager) WithRetryInterval(d time.Duration) *Manager {
	m.retryInterval = d
	return m
}

// WithMaxRetries sets the maximum number of lock acquisition attempts.
func (m *Manager) WithMaxRetries(n int) *Manager {
	m.maxRetries = n
	return m
}

// Acquire attempts to acquire the distributed lock with the given key.
// It retries up to MaxRetries times with RetryInterval between attempts.
// If the lock cannot be acquired, it returns an error describing who holds it.
//
// The returned *Lock must be released by calling lock.Release() when done.
// Always use defer:
//
//	lock, err := manager.Acquire(ctx, "schemashift_myapp")
//	if err != nil { return err }
//	defer lock.Release(ctx)
func (m *Manager) Acquire(ctx context.Context, key string) (*Lock, error) {
	// Bootstrap SQLite lock table if needed
	if m.dialect.Name() == "sqlite" {
		if err := m.ensureSQLiteLockTable(ctx); err != nil {
			return nil, err
		}
	}

	lock := &Lock{
		key:     key,
		dialect: m.dialect,
		db:      m.db,
	}

	// Pin a connection for MySQL (GET_LOCK is connection-scoped)
	if m.dialect.Name() == "mysql" {
		conn, err := m.db.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("schemashift/lock: open pinned connection: %w", err)
		}
		lock.conn = conn
	}

	// Attempt acquisition with retries
	for attempt := 1; attempt <= m.maxRetries; attempt++ {
		result, err := m.tryAcquire(ctx, lock)
		if err != nil {
			// Clean up pinned connection on error
			if lock.conn != nil {
				_ = lock.conn.Close()
			}
			return nil, fmt.Errorf("schemashift/lock: acquire attempt %d: %w", attempt, err)
		}

		switch result {
		case LockAcquired:
			lock.acquired = true
			lock.acquiredAt = time.Now()
			return lock, nil

		case LockBusy:
			if attempt == m.maxRetries {
				if lock.conn != nil {
					_ = lock.conn.Close()
				}
				return nil, fmt.Errorf(
					"schemashift/lock: could not acquire lock %q after %d attempts (%s) — "+
						"another schemashift instance may be running",
					key, m.maxRetries,
					time.Duration(m.maxRetries)*m.retryInterval,
				)
			}

			// Wait before retrying — respect context cancellation
			select {
			case <-ctx.Done():
				if lock.conn != nil {
					_ = lock.conn.Close()
				}
				return nil, fmt.Errorf(
					"schemashift/lock: context cancelled while waiting for lock %q: %w",
					key, ctx.Err(),
				)
			case <-time.After(m.retryInterval):
				// Try again
			}
		}
	}

	return nil, fmt.Errorf("schemashift/lock: exhausted retries for %q", key)
}

// tryAcquire makes a single attempt to acquire the lock.
// Returns LockAcquired, LockBusy, or LockError.
func (m *Manager) tryAcquire(ctx context.Context, lock *Lock) (LockResult, error) {
	switch m.dialect.Name() {
	case "postgres":
		return m.tryAcquirePostgres(ctx, lock)
	case "mysql":
		return m.tryAcquireMySQL(ctx, lock)
	case "sqlite":
		return m.tryAcquireSQLite(ctx, lock)
	}
	return LockError, fmt.Errorf("schemashift/lock: unsupported dialect %q", m.dialect.Name())
}

// ─── Postgres Advisory Lock ───────────────────────────────────────────────────

// tryAcquirePostgres uses pg_try_advisory_lock — non-blocking, returns immediately.
// Returns true if acquired, false if another session holds it.
func (m *Manager) tryAcquirePostgres(ctx context.Context, lock *Lock) (LockResult, error) {
	query, supported := m.dialect.AcquireLockSQL(lock.key)
	if !supported {
		return LockError, fmt.Errorf("schemashift/lock: postgres dialect does not support advisory locks")
	}

	var acquired bool
	if err := m.db.QueryRowContext(ctx, query).Scan(&acquired); err != nil {
		return LockError, fmt.Errorf("schemashift/lock: pg_try_advisory_lock: %w", err)
	}

	if acquired {
		return LockAcquired, nil
	}
	return LockBusy, nil
}

// releasePostgres releases the Postgres advisory lock.
func (m *Manager) releasePostgres(ctx context.Context, lock *Lock) error {
	query, _ := m.dialect.ReleaseLockSQL(lock.key)
	var released bool
	if err := m.db.QueryRowContext(ctx, query).Scan(&released); err != nil {
		return fmt.Errorf("schemashift/lock: pg_advisory_unlock: %w", err)
	}
	if !released {
		// Lock wasn't held by this session — log but don't error
		// This can happen if the DB connection was reset
		return fmt.Errorf("schemashift/lock: pg_advisory_unlock returned false for %q — lock may have been lost", lock.key)
	}
	return nil
}

// ─── MySQL GET_LOCK ───────────────────────────────────────────────────────────

// tryAcquireMySQL uses GET_LOCK() on the pinned connection.
// GET_LOCK returns 1=acquired, 0=timeout, NULL=error.
// We use timeout=0 so it returns immediately (non-blocking).
func (m *Manager) tryAcquireMySQL(ctx context.Context, lock *Lock) (LockResult, error) {
	if lock.conn == nil {
		return LockError, fmt.Errorf("schemashift/lock: MySQL lock requires pinned connection")
	}

	query, _ := m.dialect.AcquireLockSQL(lock.key)

	var result sql.NullInt64
	if err := lock.conn.QueryRowContext(ctx, query).Scan(&result); err != nil {
		return LockError, fmt.Errorf("schemashift/lock: GET_LOCK: %w", err)
	}

	// NULL means an error occurred in MySQL GET_LOCK
	if !result.Valid {
		return LockError, fmt.Errorf("schemashift/lock: GET_LOCK returned NULL for %q — possible DB error", lock.key)
	}

	switch result.Int64 {
	case 1:
		return LockAcquired, nil
	case 0:
		return LockBusy, nil
	default:
		return LockError, fmt.Errorf("schemashift/lock: GET_LOCK returned unexpected value %d", result.Int64)
	}
}

// releaseMySQL calls RELEASE_LOCK() on the pinned connection.
func (m *Manager) releaseMySQL(ctx context.Context, lock *Lock) error {
	if lock.conn == nil {
		return fmt.Errorf("schemashift/lock: MySQL release requires pinned connection")
	}

	query, _ := m.dialect.ReleaseLockSQL(lock.key)

	var result sql.NullInt64
	if err := lock.conn.QueryRowContext(ctx, query).Scan(&result); err != nil {
		return fmt.Errorf("schemashift/lock: RELEASE_LOCK: %w", err)
	}

	// NULL means the named lock does not exist
	if !result.Valid {
		return fmt.Errorf("schemashift/lock: RELEASE_LOCK returned NULL — lock %q may not exist", lock.key)
	}

	if result.Int64 != 1 {
		return fmt.Errorf(
			"schemashift/lock: RELEASE_LOCK returned %d for %q — lock not held by this connection",
			result.Int64, lock.key,
		)
	}

	// Close the pinned connection — it's no longer needed
	if err := lock.conn.Close(); err != nil {
		return fmt.Errorf("schemashift/lock: close pinned connection: %w", err)
	}
	lock.conn = nil

	return nil
}

// ─── SQLite Table-Based Lock ──────────────────────────────────────────────────

// ensureSQLiteLockTable creates the _schemashift_lock table if it doesn't exist.
// This table simulates advisory locks for SQLite which has no native support.
func (m *Manager) ensureSQLiteLockTable(ctx context.Context) error {
	query := `
CREATE TABLE IF NOT EXISTS _schemashift_lock (
    key         TEXT    NOT NULL PRIMARY KEY,
    acquired_at TEXT    NOT NULL DEFAULT (datetime('now')),
    holder      TEXT    NOT NULL DEFAULT ''
);`
	if _, err := m.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("schemashift/lock: create sqlite lock table: %w", err)
	}
	return nil
}

// tryAcquireSQLite attempts to insert a row into _schemashift_lock.
// SQLite's PRIMARY KEY constraint ensures only one row per key.
// INSERT OR FAIL returns an error if the row already exists — meaning locked.
func (m *Manager) tryAcquireSQLite(ctx context.Context, lock *Lock) (LockResult, error) {
	query := `INSERT OR FAIL INTO _schemashift_lock (key, holder) VALUES (?, ?);`
	holder := fmt.Sprintf("pid-%d-%s", processID(), time.Now().Format(time.RFC3339))

	_, err := m.db.ExecContext(ctx, query, lock.key, holder)
	if err != nil {
		// UNIQUE constraint violation means lock is held
		if isSQLiteConstraintError(err) {
			return LockBusy, nil
		}
		return LockError, fmt.Errorf("schemashift/lock: sqlite insert lock: %w", err)
	}

	return LockAcquired, nil
}

// releaseSQLite deletes the lock row from _schemashift_lock.
func (m *Manager) releaseSQLite(ctx context.Context, lock *Lock) error {
	query := `DELETE FROM _schemashift_lock WHERE key = ?;`
	result, err := m.db.ExecContext(ctx, query, lock.key)
	if err != nil {
		return fmt.Errorf("schemashift/lock: sqlite delete lock: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return nil // Non-fatal — some drivers don't support RowsAffected
	}
	if rows == 0 {
		return fmt.Errorf("schemashift/lock: sqlite lock %q was not held — may have been stolen", lock.key)
	}

	return nil
}

// ─── Lock Release ─────────────────────────────────────────────────────────────

// Release releases the distributed lock.
// Safe to call multiple times — subsequent calls after release are no-ops.
// Always use defer after Acquire():
//
//	lock, err := manager.Acquire(ctx, key)
//	if err != nil { return err }
//	defer lock.Release(context.Background())
func (l *Lock) Release(ctx context.Context) error {
	if !l.acquired {
		return nil // Already released or never acquired
	}

	l.acquired = false

	// Use a background manager to release — same db and dialect
	mgr := &Manager{db: l.db, dialect: l.dialect}

	var err error
	switch l.dialect.Name() {
	case "postgres":
		err = mgr.releasePostgres(ctx, l)
	case "mysql":
		err = mgr.releaseMySQL(ctx, l)
	case "sqlite":
		err = mgr.releaseSQLite(ctx, l)
	default:
		err = fmt.Errorf("schemashift/lock: unsupported dialect %q", l.dialect.Name())
	}

	if err != nil {
		return fmt.Errorf("schemashift/lock: release %q: %w", l.key, err)
	}

	return nil
}

// IsHeld returns true if this lock is currently acquired.
func (l *Lock) IsHeld() bool {
	return l.acquired
}

// Key returns the lock key.
func (l *Lock) Key() string {
	return l.key
}

// HeldFor returns how long the lock has been held.
// Returns zero if not acquired.
func (l *Lock) HeldFor() time.Duration {
	if !l.acquired {
		return 0
	}
	return time.Since(l.acquiredAt)
}

// String returns a human-readable lock description.
func (l *Lock) String() string {
	if !l.acquired {
		return fmt.Sprintf("Lock(%q, released)", l.key)
	}
	return fmt.Sprintf("Lock(%q, held for %s)", l.key, l.HeldFor().Round(time.Millisecond))
}

// ─── Lock Health Monitor ──────────────────────────────────────────────────────

// Watchdog starts a background goroutine that logs a warning if the lock
// is held longer than the given threshold. Useful for detecting stuck migrations.
// The goroutine exits when ctx is cancelled or the lock is released.
//
// Example:
//
//	lock, _ := manager.Acquire(ctx, key)
//	lock.Watchdog(ctx, 5*time.Minute, func(d time.Duration) {
//	    log.Printf("WARNING: migration lock held for %s", d)
//	})
func (l *Lock) Watchdog(ctx context.Context, threshold time.Duration, warn func(time.Duration)) {
	go func() {
		ticker := time.NewTicker(threshold / 2)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !l.acquired {
					return
				}
				held := l.HeldFor()
				if held > threshold {
					warn(held)
				}
			}
		}
	}()
}

// ─── Stale Lock Detection ─────────────────────────────────────────────────────

// StaleThreshold is how old a SQLite lock must be to be considered stale.
// Stale locks indicate a process died without releasing. Default: 1 hour.
const StaleThreshold = time.Hour

// ClearStaleLocks removes SQLite lock table entries older than StaleThreshold.
// Postgres and MySQL locks are session/connection scoped so they auto-release
// when the process dies — SQLite table locks do not, hence this utility.
//
// Safe to call on startup before attempting Acquire().
func ClearStaleLocks(ctx context.Context, db *sql.DB, d dialect.Dialect) (int64, error) {
	if d.Name() != "sqlite" {
		return 0, nil // No-op for Postgres and MySQL
	}

	// First ensure the lock table exists
	mgr := NewManager(db, d)
	if err := mgr.ensureSQLiteLockTable(ctx); err != nil {
		return 0, err
	}

	cutoff := time.Now().Add(-StaleThreshold).UTC().Format(time.RFC3339)

	result, err := db.ExecContext(ctx,
		`DELETE FROM _schemashift_lock WHERE acquired_at < ?;`,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("schemashift/lock: clear stale locks: %w", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, nil // Non-fatal
	}

	return deleted, nil
}

// InspectLock returns information about who currently holds the lock.
// Useful for debugging when Acquire() returns LockBusy.
func InspectLock(ctx context.Context, db *sql.DB, d dialect.Dialect, key string) (string, error) {
	if d.Name() != "sqlite" {
		return "", fmt.Errorf("schemashift/lock: InspectLock only supported for SQLite — Postgres/MySQL locks are opaque")
	}

	var holder string
	var acquiredAt string

	err := db.QueryRowContext(ctx,
		`SELECT holder, acquired_at FROM _schemashift_lock WHERE key = ?;`,
		key,
	).Scan(&holder, &acquiredAt)

	if err == sql.ErrNoRows {
		return "no lock held", nil
	}
	if err != nil {
		return "", fmt.Errorf("schemashift/lock: inspect: %w", err)
	}

	return fmt.Sprintf("lock %q held by %q since %s", key, holder, acquiredAt), nil
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

// isSQLiteConstraintError returns true if the error is a SQLite
// UNIQUE constraint violation — meaning the lock row already exists.
func isSQLiteConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "UNIQUE constraint failed") ||
		contains(msg, "constraint failed") ||
		contains(msg, "SQLITE_CONSTRAINT")
}

// contains is a simple substring check helper.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		len(s) > 0 && containsRune(s, substr))
}

func containsRune(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// processID returns a pseudo-unique identifier for this process.
// Used in the SQLite lock holder field for debugging.
// We use a timestamp-based ID since os.Getpid() would require importing os.
var processID = func() func() int64 {
	start := time.Now().UnixNano()
	return func() int64 { return start }
}()
