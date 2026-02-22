# schemashift

[![Go Reference](https://pkg.go.dev/badge/github.com/SravanKolanu20/schemashift.svg)](https://pkg.go.dev/github.com/SravanKolanu20/schemashift)
[![Go Report Card](https://goreportcard.com/badge/github.com/SravanKolanu20/schemashift)](https://goreportcard.com/report/github.com/SravanKolanu20/schemashift)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.22+-blue.svg)](https://golang.org/dl/)

**Zero-downtime database schema migration engine for Go.**

`schemashift` applies schema changes without locking tables — using shadow tables, dual-write strategies, and automatic rollback on failure. Works across **PostgreSQL**, **MySQL**, and **SQLite**.

---

## Why schemashift?

Standard `ALTER TABLE` operations lock your table. On a table with millions of rows, that lock can hold for minutes — taking down your app during a deploy.

`schemashift` solves this with three strategies:

| Strategy | How it works | Best for |
|---|---|---|
| **InPlace** | Runs DDL directly (metadata-only change) | `ADD COLUMN` with default, `CREATE INDEX`, `RENAME` |
| **Shadow Table** | Creates new table → copies data → atomic rename | Any change on large tables |
| **Dual Write** | Adds nullable column → backfills → adds constraint | `NOT NULL` columns, type changes |

The planner **automatically picks the right strategy** based on your dialect and table size. You can override it per-step if needed.

---

## Features

- ✅ **Zero-downtime** — live traffic continues during migrations
- ✅ **Three dialects** — PostgreSQL, MySQL, SQLite
- ✅ **Crash-safe** — checkpoints survive process restarts; automatic recovery on next startup
- ✅ **Distributed locking** — only one migrator runs at a time across all app instances
- ✅ **Automatic rollback** — on failure, completed steps are reversed in order
- ✅ **Drift detection** — warns if a migration's definition changed after being applied
- ✅ **Dry-run mode** — preview the execution plan without touching the DB
- ✅ **Observability** — hooks for logging, Prometheus metrics, Slack alerts, tracing
- ✅ **Idempotent** — safe to call on every app startup

---

## Installation

```bash
go get github.com/SravanKolanu20/schemashift
```

**Driver dependencies** (install the one you need):

```bash
# PostgreSQL
go get github.com/lib/pq

# MySQL
go get github.com/go-sql-driver/mysql

# SQLite
go get github.com/mattn/go-sqlite3
```

---

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "os"

    _ "github.com/lib/pq"
    "github.com/SravanKolanu20/schemashift"
    "github.com/SravanKolanu20/schemashift/dialect"
    "github.com/SravanKolanu20/schemashift/hooks"
    "github.com/SravanKolanu20/schemashift/migration"
)

func main() {
    db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Set up logging
    registry := hooks.NewRegistry()
    registry.OnAny(hooks.PrettyHandler(os.Stdout))

    // Create the shifter
    shifter := schemashift.New(db, dialect.NewPostgres(),
        schemashift.WithHooks(registry),
    )

    // Define migrations — ordered by version string (timestamp prefix recommended)
    migrations := []*migration.Migration{

        migration.Build("20240101_create_users").
            Describe("Create users table").
            Add(&migration.CreateTableStep{
                Table: "users",
                Columns: []dialect.ColumnDef{
                    {Name: "id",         Type: "SERIAL",    PrimaryKey: true},
                    {Name: "email",      Type: "TEXT",      Nullable: false},
                    {Name: "name",       Type: "TEXT",      Nullable: true},
                    {Name: "created_at", Type: "TIMESTAMP", Nullable: false,
                        Default: dialect.EnsureDefault("now()")},
                },
            }).
            Done(),

        migration.Build("20240102_add_phone").
            Describe("Add phone column to users").
            Add(&migration.AddColumnStep{
                Table:  "users",
                Column: dialect.ColumnDef{
                    Name:     "phone",
                    Type:     "TEXT",
                    Nullable: true,
                },
            }).
            Done(),

        migration.Build("20240103_index_email").
            Describe("Unique index on users.email").
            Add(&migration.CreateIndexStep{
                Index: dialect.IndexDef{
                    Name:    "idx_users_email",
                    Table:   "users",
                    Columns: []string{"email"},
                    Unique:  true,
                },
            }).
            Done(),
    }

    // Apply — safe to call on every startup, skips already-applied migrations
    if err := shifter.Apply(context.Background(), migrations...); err != nil {
        log.Fatalf("migration failed: %v", err)
    }
}
```

---

## Dialects

### PostgreSQL

```go
import "github.com/SravanKolanu20/schemashift/dialect"

d := dialect.NewPostgres()
shifter := schemashift.New(db, d)
```

### MySQL

```go
// dbName is required for information_schema queries
d := dialect.NewMySQL("myapp_db")
shifter := schemashift.New(db, d)
```

### SQLite

```go
d := dialect.NewSQLite()
shifter := schemashift.New(db, d)
```

---

## Migration Steps

Every migration is composed of one or more steps. Steps are executed in order and rolled back in reverse on failure.

### CreateTableStep

```go
&migration.CreateTableStep{
    Table: "orders",
    Columns: []dialect.ColumnDef{
        {Name: "id",      Type: "SERIAL",        PrimaryKey: true},
        {Name: "user_id", Type: "INTEGER",        Nullable: false},
        {Name: "total",   Type: "NUMERIC(10,2)",  Nullable: false},
        {Name: "status",  Type: "TEXT",           Nullable: false,
            Default: dialect.EnsureDefault("'pending'")},
    },
}
```

### AddColumnStep

```go
&migration.AddColumnStep{
    Table: "users",
    Column: dialect.ColumnDef{
        Name:     "verified_at",
        Type:     "TIMESTAMP",
        Nullable: true,
    },
}
```

### DropColumnStep

```go
// Requires WithAllowDestructive() on the migration
&migration.DropColumnStep{
    Table:  "users",
    Column: "legacy_token",
    Force:  true,
}
```

### RenameColumnStep

```go
&migration.RenameColumnStep{
    Table:   "users",
    OldName: "phone",
    NewName: "mobile",
}
```

### CreateIndexStep

```go
&migration.CreateIndexStep{
    Index: dialect.IndexDef{
        Name:    "idx_orders_user_id",
        Table:   "orders",
        Columns: []string{"user_id", "created_at"},
        Unique:  false,
    },
}
```

### DropIndexStep

```go
&migration.DropIndexStep{
    Table:     "orders",
    IndexName: "idx_orders_legacy",
}
```

### RenameTableStep

```go
&migration.RenameTableStep{
    OldName: "users",
    NewName: "accounts",
}
```

### RawSQLStep

```go
// Escape hatch for anything not covered by built-in steps
&migration.RawSQLStep{
    SQL:        "UPDATE users SET status = 'active' WHERE status IS NULL",
    ReverseSQL: "UPDATE users SET status = NULL WHERE status = 'active'",
}
```

---

## Migration Options

```go
migration.Build("20240101_version").
    Describe("Human readable description").
    Add(step1, step2, step3).
    BatchSize(500).               // rows per batch for shadow/dualwrite (default 1000)
    Timeout(10 * time.Minute).    // max duration (default 30m)
    AllowDestructive().           // permit DropColumn / DropTable steps
    Done()
```

Or using functional options:

```go
migration.New("20240101_version",
    migration.WithDescription("..."),
    migration.WithStep(step1, step2),
    migration.WithBatchSize(500),
    migration.WithTimeout(10 * time.Minute),
    migration.WithAllowDestructive(),
)
```

---

## Strategy Override

The planner auto-selects the safest strategy. Override per-step if needed:

```go
&migration.AddColumnStep{
    StepMeta: migration.StepMeta{
        Strategy: migration.StrategyShadow, // force shadow table
    },
    Table:  "users",
    Column: dialect.ColumnDef{Name: "score", Type: "INTEGER", Nullable: true},
}
```

Available strategies:

| Constant | Description |
|---|---|
| `migration.StrategyAuto` | Let schemashift decide (default) |
| `migration.StrategyInPlace` | Direct DDL — fastest, brief lock |
| `migration.StrategyShadow` | Shadow table — safest, slowest |
| `migration.StrategyDualWrite` | Dual write + backfill — best for NOT NULL |

---

## Shifter Options

```go
shifter := schemashift.New(db, dialect.NewPostgres(),
    schemashift.WithHooks(registry),                        // attach observability
    schemashift.WithLargeTableThreshold(500_000),           // rows before auto-shadow (default 100k)
    schemashift.WithLockKey("myapp_migrations"),            // custom advisory lock key
    schemashift.WithLockRetry(60, 2*time.Second),           // retry lock 60x every 2s
    schemashift.WithDryRun(),                               // preview plan, don't apply
    schemashift.WithContinueOnError(),                      // don't stop on first failure
    schemashift.WithDriftCheck(true),                       // warn on definition changes
)
```

---

## Observability

### Built-in Handlers

```go
registry := hooks.NewRegistry()

// Human-friendly output for development / CLI
registry.OnAny(hooks.PrettyHandler(os.Stdout))

// Structured log lines (works with any io.Writer)
registry.OnAny(hooks.LogHandler(os.Stdout))

// Throttle noisy progress events (fires every batch)
registry.OnProgress(hooks.ThrottleHandler(
    time.Second,
    hooks.LogHandler(os.Stdout),
))

// Generic metrics (plug into Prometheus, StatsD, Datadog, etc.)
registry.OnAny(hooks.MetricsHandler(
    func(name string, tags map[string]string) { /* increment counter */ },
    func(name string, val float64, tags map[string]string) { /* set gauge */ },
    func(name string, d time.Duration, tags map[string]string) { /* record timing */ },
))

// Channel-based (great for tests)
events := make(chan hooks.Event, 100)
registry.OnAny(hooks.ChannelHandler(events))
```

### Handler Combinators

```go
// Only call handler if predicate returns true
registry.OnAny(hooks.FilterHandler(
    func(e hooks.Event) bool { return e.Error != nil },
    alertHandler,
))

// Fan out to multiple handlers
registry.OnAny(hooks.MultiHandler(logHandler, metricsHandler, alertHandler))

// Rate-limit frequent events
registry.OnProgress(hooks.ThrottleHandler(time.Second, logHandler))
```

### Custom Handlers

```go
registry.OnMigrationError(func(ctx context.Context, e hooks.Event) {
    slack.Send(fmt.Sprintf("❌ Migration failed: %s\n%v",
        e.Migration.Version, e.Error,
    ))
})

registry.On([]hooks.EventKind{hooks.EventShadowCopyProgress}, func(ctx context.Context, e hooks.Event) {
    if e.BatchProgress != nil {
        fmt.Printf("Copying: %.1f%% (ETA %s)\n",
            e.BatchProgress.Percent(),
            e.BatchProgress.ETAString(),
        )
    }
})
```

---

## Rollback

Roll back the most recently applied migration:

```go
err := shifter.Rollback(ctx, migrations...)
```

> **Note:** `DropColumn` and `DropIndex` are not reversible — they return `ErrNotReversible`. Always ensure you have a database backup before applying destructive migrations.

---

## Status & Dry Run

```go
// Print migration status table
status, err := shifter.Status(ctx, migrations...)
fmt.Println(status)
// Output:
// VERSION                                  STATE        APPLIED AT                DESCRIPTION
// ─────────────────────────────────────────────────────────────────────────────────────────────
// 20240101_create_users                    ✓ applied    2024-01-01 12:00:00       Create users table
// 20240102_add_phone                       ✓ applied    2024-01-02 09:30:00       Add phone column to users
// 20240103_index_email                     ○ pending    —                         Unique index on users.email

// Preview the execution plan without applying
plan, err := shifter.Plan(ctx, migrations...)
fmt.Println(plan.Summary())

// Or use WithDryRun option
shifter := schemashift.New(db, d, schemashift.WithDryRun())
shifter.Apply(ctx, migrations...) // prints plan, applies nothing
```

---

## Drift Detection

`schemashift` stores a checksum of each migration when it's applied. If the migration definition changes afterward, drift is detected on the next run:

```go
report, err := shifter.CheckDrift(ctx, migrations...)
if report != "" {
    log.Warn(report)
    // DRIFT: migration "20240101_create_users" was applied on 2024-01-01 with 2 steps
    // (checksum: a1b2c3d4) but now has 3 steps (checksum: e5f6a7b8)
}
```

---

## Crash Safety

`schemashift` saves a checkpoint before, during, and after every step. If the process crashes mid-migration, the next startup automatically detects the incomplete state and rolls it back:

```
Process A: migration "20240101" starts
Process A: step 1 complete — checkpoint saved
Process A: step 2 complete — checkpoint saved
Process A: CRASHES during step 3

Process B starts (next deploy):
  → schemashift detects "20240101" has started checkpoint but no complete checkpoint
  → automatically rolls back steps 2 and 1
  → migration is now clean and pending
  → plan is rebuilt and migration runs from the beginning
```

---

## Internal Tables

`schemashift` creates three internal tables on first use:

| Table | Purpose |
|---|---|
| `_schemashift_migrations` | Permanent record of applied migrations |
| `_schemashift_checkpoints` | Crash-safe execution state |
| `_schemashift_lock` | SQLite-only distributed lock (Postgres/MySQL use advisory locks) |

These tables are created automatically — no manual setup required.

---

## Project Structure

```
schemashift/
├── schemashift.go          ← Main public API (Shifter)
├── go.mod
├── dialect/
│   ├── dialect.go          ← Dialect interface + shared helpers
│   ├── postgres.go         ← PostgreSQL implementation
│   ├── mysql.go            ← MySQL implementation
│   └── sqlite.go           ← SQLite implementation
├── migration/
│   ├── step.go             ← Step interface + all built-in steps
│   ├── migration.go        ← Migration struct + lifecycle
│   ├── plan.go             ← Execution planner + strategy resolution
│   └── checkpoint.go       ← Crash-safe checkpoint persistence
├── strategy/
│   ├── strategy.go         ← Strategy interface + dispatch
│   ├── inplace.go          ← InPlace executor
│   ├── shadow.go           ← Shadow table executor
│   └── dualwrite.go        ← Dual-write executor
├── tracker/
│   ├── schema.go           ← Migration history table DDL
│   └── tracker.go          ← Applied migration tracker + drift detection
├── lock/
│   └── lock.go             ← Distributed lock manager
├── hooks/
│   └── hooks.go            ← Event system + built-in handlers
└── examples/
    ├── postgres/main.go
    ├── mysql/main.go
    └── sqlite/main.go
```

---

## Version Naming Convention

Migration versions must be unique and are applied in ascending lexicographic order. The recommended convention is a timestamp prefix:

```
YYYYMMDDHHMMSS_short_description
20240101120000_create_users
20240115093000_add_phone_to_users
20240201140000_index_users_email
```

This guarantees correct ordering and gives you a human-readable audit trail.

---

## Requirements

- Go 1.22+
- PostgreSQL 11+ / MySQL 8.0+ / SQLite 3.25+

---

## Contributing

Contributions are welcome. Please open an issue before submitting a PR for significant changes.

```bash
git clone https://github.com/SravanKolanu20/schemashift
cd schemashift
go test ./...
```

---

## License

MIT — see [LICENSE](LICENSE) for details.
