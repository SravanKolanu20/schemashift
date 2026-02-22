package hooks

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/SravanKolanu20/schemashift/migration"
)

// EventKind identifies what type of event occurred.
type EventKind string

const (
	// Lifecycle events
	EventShifterStart    EventKind = "shifter.start"    // Shifter.Apply() called
	EventShifterComplete EventKind = "shifter.complete" // All migrations done
	EventShifterError    EventKind = "shifter.error"    // Shifter.Apply() failed

	// Plan events
	EventPlanBuilt EventKind = "plan.built" // Execution plan created

	// Lock events
	EventLockAcquiring EventKind = "lock.acquiring" // About to try acquiring
	EventLockAcquired  EventKind = "lock.acquired"  // Lock successfully held
	EventLockReleased  EventKind = "lock.released"  // Lock released
	EventLockBusy      EventKind = "lock.busy"      // Lock held by another — retrying

	// Migration events
	EventMigrationStart    EventKind = "migration.start"    // Single migration starting
	EventMigrationComplete EventKind = "migration.complete" // Single migration done
	EventMigrationSkipped  EventKind = "migration.skipped"  // Already applied — skipped
	EventMigrationRollback EventKind = "migration.rollback" // Rollback triggered
	EventMigrationError    EventKind = "migration.error"    // Migration failed

	// Step events
	EventStepStart    EventKind = "step.start"    // Individual step starting
	EventStepComplete EventKind = "step.complete" // Individual step done
	EventStepError    EventKind = "step.error"    // Individual step failed

	// Strategy events
	EventShadowCopyStart    EventKind = "shadow.copy.start"    // Batch copy starting
	EventShadowCopyProgress EventKind = "shadow.copy.progress" // Batch copy progress
	EventShadowCopyComplete EventKind = "shadow.copy.complete" // Batch copy done
	EventShadowRename       EventKind = "shadow.rename"        // Shadow table renamed

	// Drift events
	EventDriftDetected EventKind = "drift.detected" // Migration definition changed
)

// Event carries all information about something that happened
// during a migration run. Passed to every registered handler.
type Event struct {
	// Kind is what type of event this is.
	Kind EventKind

	// At is when the event occurred.
	At time.Time

	// Migration is the migration involved, if applicable.
	// Nil for top-level shifter events.
	Migration *migration.Migration

	// StepIndex is the 0-based index of the step involved.
	// -1 if not step-specific.
	StepIndex int

	// StepKind is the kind of step involved, if applicable.
	StepKind migration.StepKind

	// Strategy is the resolved strategy for this step, if applicable.
	Strategy migration.StrategyHint

	// Duration is how long the operation took (for complete/error events).
	Duration time.Duration

	// Error is the error that occurred (for error events).
	Error error

	// BatchProgress carries batch copy progress for shadow copy events.
	BatchProgress *BatchProgress

	// Message is a human-readable description of the event.
	Message string

	// Meta carries arbitrary extra data attached by the emitter.
	Meta map[string]any
}

// BatchProgress carries progress information for shadow table batch copy.
type BatchProgress struct {
	// Table is the source table being copied.
	Table string

	// ShadowTable is the destination shadow table.
	ShadowTable string

	// RowsCopied is how many rows have been copied so far.
	RowsCopied int64

	// TotalRows is the estimated total row count.
	TotalRows int64

	// BatchNumber is the current batch (1-indexed).
	BatchNumber int

	// BatchSize is how many rows per batch.
	BatchSize int

	// ElapsedTime is how long the copy has been running.
	ElapsedTime time.Duration

	// RowsPerSecond is the current copy throughput.
	RowsPerSecond float64
}

// Percent returns copy progress as a 0-100 float.
func (bp *BatchProgress) Percent() float64 {
	if bp.TotalRows == 0 {
		return 0
	}
	return float64(bp.RowsCopied) / float64(bp.TotalRows) * 100
}

// ETAString returns a human-readable estimated time remaining.
func (bp *BatchProgress) ETAString() string {
	if bp.RowsPerSecond <= 0 || bp.TotalRows == 0 {
		return "unknown"
	}
	remaining := float64(bp.TotalRows-bp.RowsCopied) / bp.RowsPerSecond
	return time.Duration(remaining * float64(time.Second)).Round(time.Second).String()
}

// String returns a human-readable progress line.
func (bp *BatchProgress) String() string {
	return fmt.Sprintf(
		"%s → %s: %s / %s rows (%.1f%%) batch %d @ %.0f rows/s ETA %s",
		bp.Table,
		bp.ShadowTable,
		formatInt(bp.RowsCopied),
		formatInt(bp.TotalRows),
		bp.Percent(),
		bp.BatchNumber,
		bp.RowsPerSecond,
		bp.ETAString(),
	)
}

// Handler is a function that receives migration events.
// Handlers must be non-blocking — heavy work should be done in a goroutine.
// The context passed is the migration's context — check ctx.Done() if needed.
type Handler func(ctx context.Context, event Event)

// ─── Registry ─────────────────────────────────────────────────────────────────

// Registry holds all registered event handlers and dispatches events to them.
// It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[EventKind][]Handler
	global   []Handler // Global handlers receive every event regardless of kind
}

// NewRegistry creates a new empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[EventKind][]Handler),
	}
}

// On registers a handler for one or more specific event kinds.
// A handler registered for multiple kinds is called once per matching event.
//
// Example:
//
//	hooks.On(hooks.EventMigrationStart, hooks.EventMigrationComplete, func(ctx context.Context, e hooks.Event) {
//	    log.Printf("[migration] %s: %s", e.Kind, e.Migration.Version)
//	})
func (r *Registry) On(kinds []EventKind, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, kind := range kinds {
		r.handlers[kind] = append(r.handlers[kind], h)
	}
}

// OnAny registers a global handler that receives every event.
//
// Example:
//
//	hooks.OnAny(func(ctx context.Context, e hooks.Event) {
//	    metrics.Increment("schemashift.events", map[string]string{"kind": string(e.Kind)})
//	})
func (r *Registry) OnAny(h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.global = append(r.global, h)
}

// Emit dispatches an event to all registered handlers.
// Handlers are called synchronously in registration order.
// If a handler panics, the panic is recovered and logged — it does not
// propagate to the migration execution.
func (r *Registry) Emit(ctx context.Context, event Event) {
	// Set timestamp if not already set
	if event.At.IsZero() {
		event.At = time.Now()
	}

	r.mu.RLock()
	specific := r.handlers[event.Kind]
	global := r.global
	r.mu.RUnlock()

	// Call specific handlers first, then global handlers
	for _, h := range specific {
		safeCall(ctx, h, event)
	}
	for _, h := range global {
		safeCall(ctx, h, event)
	}
}

// EmitSimple emits an event with just a kind and message — convenience wrapper.
func (r *Registry) EmitSimple(ctx context.Context, kind EventKind, msg string) {
	r.Emit(ctx, Event{
		Kind:    kind,
		Message: msg,
	})
}

// EmitMigrationEvent emits a migration lifecycle event.
func (r *Registry) EmitMigrationEvent(
	ctx context.Context,
	kind EventKind,
	m *migration.Migration,
	duration time.Duration,
	err error,
) {
	msg := buildMigrationMessage(kind, m, duration, err)
	r.Emit(ctx, Event{
		Kind:      kind,
		Migration: m,
		Duration:  duration,
		Error:     err,
		Message:   msg,
	})
}

// EmitStepEvent emits a step lifecycle event.
func (r *Registry) EmitStepEvent(
	ctx context.Context,
	kind EventKind,
	m *migration.Migration,
	stepIndex int,
	stepKind migration.StepKind,
	strategy migration.StrategyHint,
	duration time.Duration,
	err error,
) {
	r.Emit(ctx, Event{
		Kind:      kind,
		Migration: m,
		StepIndex: stepIndex,
		StepKind:  stepKind,
		Strategy:  strategy,
		Duration:  duration,
		Error:     err,
		Message: fmt.Sprintf(
			"migration %q step %d (%s) [%s]: %s",
			m.Version, stepIndex+1, stepKind, strategy, eventVerb(kind),
		),
	})
}

// EmitBatchProgress emits a shadow copy progress event.
func (r *Registry) EmitBatchProgress(ctx context.Context, m *migration.Migration, bp *BatchProgress) {
	r.Emit(ctx, Event{
		Kind:          EventShadowCopyProgress,
		Migration:     m,
		BatchProgress: bp,
		Message:       bp.String(),
	})
}

// ─── Safe Dispatch ────────────────────────────────────────────────────────────

// safeCall calls a handler and recovers from any panic it throws.
// This ensures a buggy handler never breaks migration execution.
func safeCall(ctx context.Context, h Handler, event Event) {
	defer func() {
		if rec := recover(); rec != nil {
			// Can't do much here without a logger — print to stderr
			fmt.Printf("schemashift/hooks: handler panic recovered: %v\n", rec)
		}
	}()
	h(ctx, event)
}

// ─── Built-in Handlers ────────────────────────────────────────────────────────

// LogHandler returns a Handler that writes structured log lines to w.
// Format: timestamp | level | kind | message
//
// Example:
//
//	registry.OnAny(hooks.LogHandler(os.Stdout))
func LogHandler(w io.Writer) Handler {
	return func(ctx context.Context, e Event) {
		level := "INFO"
		if e.Error != nil {
			level = "ERROR"
		}

		ts := e.At.Format("2006-01-02T15:04:05.000Z")

		var sb strings.Builder
		fmt.Fprintf(&sb, "%s | %-5s | %-30s | %s", ts, level, e.Kind, e.Message)

		if e.Error != nil {
			fmt.Fprintf(&sb, " | error=%q", e.Error.Error())
		}

		if e.Migration != nil && e.Duration > 0 {
			fmt.Fprintf(&sb, " | duration=%s", e.Duration.Round(time.Millisecond))
		}

		fmt.Fprintln(w, sb.String())
	}
}

// PrettyHandler returns a Handler that writes human-friendly colored output.
// Best used in CLI tools and development environments.
//
// Example:
//
//	registry.OnAny(hooks.PrettyHandler(os.Stdout))
func PrettyHandler(w io.Writer) Handler {
	return func(ctx context.Context, e Event) {
		switch e.Kind {

		case EventShifterStart:
			fmt.Fprintf(w, "\n🚀 schemashift starting...\n\n")

		case EventShifterComplete:
			fmt.Fprintf(w, "\n✅ schemashift complete in %s\n\n", e.Duration.Round(time.Millisecond))

		case EventShifterError:
			fmt.Fprintf(w, "\n❌ schemashift failed: %v\n\n", e.Error)

		case EventLockAcquired:
			fmt.Fprintf(w, "🔒 lock acquired: %s\n", e.Message)

		case EventLockReleased:
			fmt.Fprintf(w, "🔓 lock released: %s\n", e.Message)

		case EventLockBusy:
			fmt.Fprintf(w, "⏳ waiting for lock: %s\n", e.Message)

		case EventPlanBuilt:
			fmt.Fprintf(w, "%s\n", e.Message)

		case EventMigrationStart:
			if e.Migration != nil {
				fmt.Fprintf(w, "  ▶ [%s] %s\n", e.Migration.Version, e.Migration.Description)
			}

		case EventMigrationComplete:
			if e.Migration != nil {
				fmt.Fprintf(w, "  ✓ [%s] done in %s\n", e.Migration.Version, e.Duration.Round(time.Millisecond))
			}

		case EventMigrationSkipped:
			if e.Migration != nil {
				fmt.Fprintf(w, "  ○ [%s] already applied — skipping\n", e.Migration.Version)
			}

		case EventMigrationError:
			if e.Migration != nil {
				fmt.Fprintf(w, "  ✗ [%s] failed: %v\n", e.Migration.Version, e.Error)
			}

		case EventMigrationRollback:
			if e.Migration != nil {
				fmt.Fprintf(w, "  ↩ [%s] rolling back...\n", e.Migration.Version)
			}

		case EventStepStart:
			fmt.Fprintf(w, "      → step %d: %s [%s]\n",
				e.StepIndex+1, e.StepKind, e.Strategy,
			)

		case EventStepComplete:
			fmt.Fprintf(w, "      ✓ step %d: %s done in %s\n",
				e.StepIndex+1, e.StepKind, e.Duration.Round(time.Millisecond),
			)

		case EventStepError:
			fmt.Fprintf(w, "      ✗ step %d: %s failed: %v\n",
				e.StepIndex+1, e.StepKind, e.Error,
			)

		case EventShadowCopyProgress:
			if e.BatchProgress != nil {
				fmt.Fprintf(w, "      ⟳ %s\n", e.BatchProgress)
			}

		case EventShadowRename:
			fmt.Fprintf(w, "      ⇄ %s\n", e.Message)

		case EventDriftDetected:
			fmt.Fprintf(w, "  ⚠ DRIFT: %s\n", e.Message)
		}
	}
}

// MetricsHandler returns a Handler that calls your metrics functions
// on key migration events. Pass nil for any metric you don't need.
//
// Example:
//
//	registry.OnAny(hooks.MetricsHandler(
//	    func(name string, tags map[string]string) { statsd.Increment(name, tags) },
//	    func(name string, val float64, tags map[string]string) { statsd.Gauge(name, val, tags) },
//	    func(name string, d time.Duration, tags map[string]string) { statsd.Timing(name, d, tags) },
//	))
func MetricsHandler(
	increment func(name string, tags map[string]string),
	gauge func(name string, val float64, tags map[string]string),
	timing func(name string, d time.Duration, tags map[string]string),
) Handler {
	return func(ctx context.Context, e Event) {
		version := "unknown"
		if e.Migration != nil {
			version = e.Migration.Version
		}

		tags := map[string]string{
			"version": version,
			"kind":    string(e.Kind),
		}

		switch e.Kind {

		case EventMigrationStart:
			if increment != nil {
				increment("schemashift.migration.started", tags)
			}

		case EventMigrationComplete:
			if increment != nil {
				increment("schemashift.migration.completed", tags)
			}
			if timing != nil && e.Duration > 0 {
				timing("schemashift.migration.duration", e.Duration, tags)
			}

		case EventMigrationError:
			if increment != nil {
				increment("schemashift.migration.failed", tags)
			}

		case EventMigrationRollback:
			if increment != nil {
				increment("schemashift.migration.rollback", tags)
			}

		case EventStepComplete:
			stepTags := map[string]string{
				"version":  version,
				"step":     string(e.StepKind),
				"strategy": e.Strategy.String(),
			}
			if timing != nil && e.Duration > 0 {
				timing("schemashift.step.duration", e.Duration, stepTags)
			}

		case EventShadowCopyProgress:
			if e.BatchProgress != nil && gauge != nil {
				progressTags := map[string]string{
					"version": version,
					"table":   e.BatchProgress.Table,
				}
				gauge("schemashift.shadow.progress_pct", e.BatchProgress.Percent(), progressTags)
				gauge("schemashift.shadow.rows_per_second", e.BatchProgress.RowsPerSecond, progressTags)
			}

		case EventDriftDetected:
			if increment != nil {
				increment("schemashift.drift.detected", tags)
			}
		}
	}
}

// ChannelHandler returns a Handler that sends events to a Go channel.
// Useful for testing — receive events and assert on them.
// The channel should be buffered to avoid blocking migration execution.
//
// Example:
//
//	events := make(chan hooks.Event, 100)
//	registry.OnAny(hooks.ChannelHandler(events))
//	// ... run migrations ...
//	for e := range events { ... }
func ChannelHandler(ch chan<- Event) Handler {
	return func(ctx context.Context, e Event) {
		select {
		case ch <- e:
		default:
			// Channel full — drop event rather than blocking migration
		}
	}
}

// FilterHandler wraps a Handler and only calls it if the filter function returns true.
// Useful for routing different events to different destinations.
//
// Example — only handle error events:
//
//	registry.OnAny(hooks.FilterHandler(
//	    func(e hooks.Event) bool { return e.Error != nil },
//	    hooks.LogHandler(os.Stderr),
//	))
func FilterHandler(filter func(Event) bool, h Handler) Handler {
	return func(ctx context.Context, e Event) {
		if filter(e) {
			h(ctx, e)
		}
	}
}

// MultiHandler combines multiple handlers into one.
// All handlers are called for every event.
//
// Example:
//
//	registry.OnAny(hooks.MultiHandler(
//	    hooks.LogHandler(os.Stdout),
//	    hooks.MetricsHandler(...),
//	    alertHandler,
//	))
func MultiHandler(handlers ...Handler) Handler {
	return func(ctx context.Context, e Event) {
		for _, h := range handlers {
			safeCall(ctx, h, e)
		}
	}
}

// ThrottleHandler wraps a Handler and rate-limits calls to at most once per interval.
// Useful for progress events that fire very frequently.
//
// Example — log shadow copy progress at most once per second:
//
//	registry.On([]hooks.EventKind{hooks.EventShadowCopyProgress},
//	    hooks.ThrottleHandler(time.Second, hooks.LogHandler(os.Stdout)),
//	)
func ThrottleHandler(interval time.Duration, h Handler) Handler {
	var mu sync.Mutex
	var lastCall time.Time

	return func(ctx context.Context, e Event) {
		mu.Lock()
		now := time.Now()
		if now.Sub(lastCall) < interval {
			mu.Unlock()
			return
		}
		lastCall = now
		mu.Unlock()

		h(ctx, e)
	}
}

// ─── Convenience Constructors ─────────────────────────────────────────────────

// OnMigrationStart is a convenience for registering a handler on migration start.
func (r *Registry) OnMigrationStart(h Handler) {
	r.On([]EventKind{EventMigrationStart}, h)
}

// OnMigrationComplete is a convenience for registering a handler on migration complete.
func (r *Registry) OnMigrationComplete(h Handler) {
	r.On([]EventKind{EventMigrationComplete}, h)
}

// OnMigrationError is a convenience for registering a handler on migration error.
func (r *Registry) OnMigrationError(h Handler) {
	r.On([]EventKind{EventMigrationError}, h)
}

// OnStepComplete is a convenience for registering a handler on step complete.
func (r *Registry) OnStepComplete(h Handler) {
	r.On([]EventKind{EventStepComplete}, h)
}

// OnDrift is a convenience for registering a handler when schema drift is detected.
func (r *Registry) OnDrift(h Handler) {
	r.On([]EventKind{EventDriftDetected}, h)
}

// OnProgress is a convenience for registering a handler on shadow copy progress.
func (r *Registry) OnProgress(h Handler) {
	r.On([]EventKind{EventShadowCopyProgress}, h)
}

// ─── Internal Helpers ─────────────────────────────────────────────────────────

// buildMigrationMessage constructs a human-readable message for migration events.
func buildMigrationMessage(kind EventKind, m *migration.Migration, d time.Duration, err error) string {
	if m == nil {
		return string(kind)
	}

	switch kind {
	case EventMigrationStart:
		return fmt.Sprintf("migration %q starting (%d steps)", m.Version, len(m.Steps))
	case EventMigrationComplete:
		return fmt.Sprintf("migration %q complete in %s", m.Version, d.Round(time.Millisecond))
	case EventMigrationSkipped:
		return fmt.Sprintf("migration %q already applied — skipping", m.Version)
	case EventMigrationRollback:
		return fmt.Sprintf("migration %q rolling back", m.Version)
	case EventMigrationError:
		if err != nil {
			return fmt.Sprintf("migration %q failed: %v", m.Version, err)
		}
		return fmt.Sprintf("migration %q failed", m.Version)
	}

	return fmt.Sprintf("migration %q: %s", m.Version, kind)
}

// eventVerb returns a past-tense verb for an event kind.
func eventVerb(kind EventKind) string {
	switch kind {
	case EventStepStart:
		return "starting"
	case EventStepComplete:
		return "complete"
	case EventStepError:
		return "failed"
	default:
		return string(kind)
	}
}

// formatInt formats an integer with thousands separators.
func formatInt(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%s,%03d", formatInt(n/1000), n%1000)
}
