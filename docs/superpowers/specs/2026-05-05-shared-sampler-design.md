# Shared Sampler + macOS CPU Truthfulness

**Date:** 2026-05-05
**Status:** Approved, pending implementation plan
**Scope:** Refactor backend sampling to a single shared producer; replace synthetic macOS CPU total with real `top`-driven instant utilization; add multi-client SSE fan-out with slow-consumer handling; add graceful shutdown.

## Problem

Three concrete defects in the current design:

1. **Multi-client races corrupt deltas.** `state` in `state.go` is a process-global singleton holding `previousCPU`, `previousNet`, `previousAt`. Each SSE handler in `server.go` runs its own 1 Hz ticker and calls `sampleSystem()`, which read-replaces those fields. Two concurrent clients clobber each other's previous-counter snapshots; CPU% and network rate become noise.
2. **macOS CPU total is not instant utilization.** `darwinCPUUsage()` sums `ps -A -o %cpu=`, which reports each process's *cumulative* average since it started. The number drifts slowly and does not reflect current load. `visualPerCore()` admits in code that per-core values are a sine-wave decoration.
3. **Subprocess fan-out scales with client count.** Each tick of each client forks ~10 processes (`ps`, `df`, `lsof`, `sysctl`, `vm_stat`, `git`, …). Static facts (`hostname`, `cpuModel`, `platformName`, `arch`) are re-fetched every second despite never changing.

Operationally, there is no graceful shutdown — `Ctrl-C` kills SSE writes mid-flush.

## Goals

- One sampling pipeline shared by all `/events` clients and `/api/snapshot`.
- macOS CPU total reflects instant utilization, not cumulative averages.
- Per-core values on macOS remain visually present but are explicitly marked estimated, both in the JSON shape and in the UI.
- Slow SSE consumers cannot stall the sampler or other clients.
- `SIGINT`/`SIGTERM` shuts the server down cleanly.
- No new external Go dependencies; no `cgo`. Single-binary distribution preserved.
- Snapshot JSON shape stays additive-only (one new field on `CPU`).

## Non-goals

- Persistent history across restarts (deferred to feature stream C).
- Authentication / public exposure (deferred; server still binds `127.0.0.1` by default).
- True per-core CPU on macOS (would require `cgo` + Mach `host_processor_info`).
- Reducing duplicate `ps` invocations (different `-o` formats; future optimization).

## Design

### Module layout

A new `sampler.go` owns sampling, fan-out, and previous-counter state. `state.go` is removed; the `appState` struct in `types.go` is replaced by `Sampler` defined in `sampler.go`. `main.go` constructs the sampler and passes it to handlers.

```go
// sampler.go
type Sampler struct {
    mu          sync.Mutex
    latest      atomic.Pointer[Snapshot]
    subscribers map[chan *Snapshot]*subState
    host        Host                       // computed once at startup
    previousCPU []cpuTimes
    previousNet map[string]netCounters
    previousAt  time.Time
    eventID     int64
    events      []Event
    darwinCPU   atomic.Pointer[darwinCPU]  // updated by top streamer goroutine
}

type subState struct {
    drops int
}

type darwinCPU struct {
    total float64
    at    time.Time
}
```

Public API:

```go
func New() *Sampler
func (s *Sampler) Run(ctx context.Context) error
func (s *Sampler) Latest() *Snapshot
func (s *Sampler) Subscribe() (<-chan *Snapshot, func())
```

### Lifecycle

`main.go`:

```go
ctx, stop := signal.NotifyContext(context.Background(),
    syscall.SIGINT, syscall.SIGTERM)
defer stop()

sampler := New()
go sampler.Run(ctx)

server := &http.Server{Addr: addr, Handler: mux}
go func() {
    <-ctx.Done()
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _ = server.Shutdown(shutdownCtx)
}()

if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
    log.Fatal(err)
}
```

`Sampler.Run`:

1. Compute the static fields of `Host` once and cache on `s.host`: `Name`, `Platform`, `Arch`, `Node`. `Uptime` is recomputed each tick and overlaid when copying `s.host` into the snapshot.
2. On Darwin, spawn a goroutine that runs `top -l 0 -s 1 -n 0` as a long-lived child process, scans stdout for `CPU usage:` lines, parses `user + sys`, and stores into `s.darwinCPU` atomically. Process is killed when `ctx` is cancelled.
3. Start `time.NewTicker(time.Second)`.
4. Each tick: `snap := s.sample()`, `s.latest.Store(&snap)`, `s.broadcast(&snap)`.
5. On `<-ctx.Done()`: stop ticker, close all subscriber channels, return.

### Sampling

`s.sample()` is the renamed/migrated body of today's `sampleSystem()`. Differences:

- Reads/writes `previousCPU`, `previousNet`, `previousAt` on `s` under `s.mu`.
- `Host` populated by copying `s.host`, then overwriting `Uptime` with the per-tick value.
- CPU branch on Darwin: `cpu.Total = s.darwinCPU.Load().total` (with fallback to `loadFallback()` if streamer hasn't produced a sample yet); `cpu.PerCore = visualPerCore(cpu.Total, cores)`; new field `cpu.PerCoreEstimated = true`. Linux path unchanged; sets `PerCoreEstimated = false`.
- Event ring (`s.events`, `s.eventID`) lives on `s`; `updateEvents()` becomes a method `s.updateEvents(snap)` taking the lock the caller already holds (refactor: do not re-lock).

### Fan-out and slow-consumer handling

```go
func (s *Sampler) Subscribe() (<-chan *Snapshot, func()) {
    ch := make(chan *Snapshot, 1)
    s.mu.Lock()
    s.subscribers[ch] = &subState{}
    s.mu.Unlock()
    return ch, func() {
        s.mu.Lock()
        if _, ok := s.subscribers[ch]; ok {
            delete(s.subscribers, ch)
            close(ch)
        }
        s.mu.Unlock()
    }
}

func (s *Sampler) broadcast(snap *Snapshot) {
    s.mu.Lock()
    defer s.mu.Unlock()
    for ch, st := range s.subscribers {
        select {
        case ch <- snap:
            st.drops = 0
        default:
            // Replace stale buffered snapshot with newest.
            select { case <-ch: default: }
            select {
            case ch <- snap:
                st.drops++
            default:
                st.drops++
            }
            if st.drops >= 3 {
                delete(s.subscribers, ch)
                close(ch)
            }
        }
    }
}
```

Semantics:

- Channel capacity 1: a slow client always sees newest snapshot, never a queue of stale ones.
- Three consecutive missed ticks → server closes the channel; handler's range exits and the connection is dropped.
- `unsubscribe` is idempotent against `broadcast`-driven close (checks for presence before closing).

### Handler changes

`server.go`:

```go
func eventsHandler(s *Sampler) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // headers + flusher check unchanged
        sendSSE(w, "ready", map[string]bool{"ok": true})
        flusher.Flush()

        if snap := s.Latest(); snap != nil {
            sendSSE(w, "metrics", snap)
            flusher.Flush()
        }

        ch, unsub := s.Subscribe()
        defer unsub()

        for {
            select {
            case <-r.Context().Done():
                return
            case snap, ok := <-ch:
                if !ok {
                    return
                }
                sendSSE(w, "metrics", snap)
                flusher.Flush()
            }
        }
    }
}

func snapshotHandler(s *Sampler) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        snap := s.Latest()
        if snap == nil {
            w.Header().Set("Retry-After", "1")
            http.Error(w, "sampler warming up", http.StatusServiceUnavailable)
            return
        }
        writeJSON(w, snap)
    }
}
```

`main.go` registers `mux.Handle("/events", eventsHandler(sampler))` etc.

### macOS CPU streamer

```go
func (s *Sampler) runDarwinTopStreamer(ctx context.Context) {
    cmd := exec.CommandContext(ctx, "top", "-l", "0", "-s", "1", "-n", "0")
    stdout, err := cmd.StdoutPipe()
    if err != nil {
        return
    }
    if err := cmd.Start(); err != nil {
        return
    }
    defer cmd.Wait()

    scanner := bufio.NewScanner(stdout)
    re := regexp.MustCompile(`CPU usage:\s*([\d.]+)%\s*user,\s*([\d.]+)%\s*sys`)
    for scanner.Scan() {
        match := re.FindStringSubmatch(scanner.Text())
        if len(match) != 3 {
            continue
        }
        s.darwinCPU.Store(&darwinCPU{
            total: clamp(parseFloat(match[1])+parseFloat(match[2]), 0, 100),
            at:    time.Now(),
        })
    }
}
```

Notes:

- `top -l 0` streams forever, emitting one sample per `-s 1` interval. The first emitted `CPU usage:` line is `top`'s "since boot" cumulative average — skip it (track a `firstSampleSeen` bool, ignore the first regex match). Subsequent samples are instant deltas.
- When `ctx` is cancelled, `exec.CommandContext` SIGKILLs `top`; `Wait()` returns; goroutine exits.
- If `top` is unavailable or exits early, `s.darwinCPU.Load()` returns `nil` and the sampler falls back to `loadFallback()` for CPU total.

### Snapshot shape change

```go
type CPU struct {
    Total            float64   `json:"total"`
    PerCore          []float64 `json:"perCore"`
    PerCoreEstimated bool      `json:"perCoreEstimated"` // NEW
    Cores            int       `json:"cores"`
    Model            string    `json:"model"`
    LoadAverage     []float64  `json:"loadAverage"`
}
```

`PerCoreEstimated == true` only on Darwin. Linux always emits `false`.

### Frontend

`public/app.js`:

- `renderCores(cores, estimated)` accepts the new flag.
- When `estimated == true`, set `data-estimated="true"` on `#coreGrid`; styles.css adds a `.core-grid[data-estimated="true"]` rule that reduces block opacity to 0.6.
- Append `<small>(estimated)</small>` to the CPU `metric-card__header` (`article[data-metric="cpu"] .metric-card__header`) when estimated; remove it when not. The frontend handles both branches because the same client may reconnect to a Linux box later.

EventSource and connection-badge logic unchanged. SSE handlers send `ready` before the first `metrics`, and `Latest()` is sent immediately on subscribe if a snapshot exists, so perceived warmup is identical to today. The 503 window only affects manual hits to `/api/snapshot` during the sub-second startup gap.

## Cross-cutting concerns

- **Locking discipline:** `s.mu` protects `previousCPU`, `previousNet`, `previousAt`, `events`, `eventID`, `subscribers`. `latest` and `darwinCPU` are `atomic.Pointer` and don't need the mutex. `host` is set once before `Run` starts and read-only afterward.
- **No reentrancy:** `s.sample()` and `s.broadcast()` both take `s.mu`; `broadcast` is called outside `sample`. `s.updateEvents()` is called from `sample()` and assumes the lock is already held — name it accordingly (`s.updateEventsLocked()`) and document.
- **Errors:** `Run` returns `nil` on clean shutdown, error only if startup of the Darwin streamer wedges in a way we can't tolerate. For now, treat streamer failure as recoverable (fall back to `loadFallback`); log once at WARN and continue.
- **Shutdown ordering:** `signal.NotifyContext` cancels `ctx`. `Sampler.Run` observes the cancel, stops the ticker, takes `s.mu` once to close every subscriber channel and clear the map, then returns. Each SSE handler's range receives the close (`ok=false`), exits, and `server.Shutdown`'s 5s grace handles the rest.

## Migration plan (for the implementation plan, not this spec)

The implementation plan will sequence:

1. Introduce `Sampler` type alongside existing code; keep `state` global; verify compile.
2. Move sampling logic onto `Sampler`; route handlers to use it.
3. Delete `state` global and `state.go`.
4. Add Darwin `top` streamer; replace `darwinCPUUsage` callsite.
5. Add `PerCoreEstimated` field; thread through frontend.
6. Add graceful shutdown to `main.go`.
7. Add a small concurrent-clients smoke test (`net/http/httptest` + two SSE consumers) to prove deltas no longer race.

## Open questions

None. All gating decisions confirmed in design conversation:

- Sampler is always-on (not lazy).
- Slow client policy: replace-on-overflow + close after 3 consecutive drops.
- `/api/snapshot` returns 503 during warmup, not a trigger-sample fallback.
- macOS uses long-lived `top -l 0` streamer, not one-shot `top -l 2`.
- `PerCoreEstimated` lives on `CPU`, not `Health`.
