# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`jsys` is a single-binary, zero-dependency Go service that samples the local machine and streams metrics to a browser UI ("Jerry's local system command center"). Backend module `github.com/jcdkuo/jsys`, Go 1.24. No external Go dependencies; the frontend is vanilla JS modules with no build step.

## Commands

```sh
go run .                        # run dev server on http://127.0.0.1:4173
PORT=5000 HOST=0.0.0.0 go run . # override bind address/port
go build -o jsys . && ./jsys    # produce single binary
go vet ./...                    # static analysis
go test ./...                   # run all tests
go test -run TestName ./...     # run a single test
go fmt ./...                    # format
```

Tests live in `sampler_test.go` (subscribe/broadcast/run lifecycle, multi-client SSE integration, Darwin top parser).

## Architecture

### Request flow

`main.go` constructs a single `Sampler`, runs it as a background goroutine, and wires three routes on `http.ServeMux`:

- `GET /` → serves `public/` via `http.FileServer`
- `GET /api/snapshot` → one-shot JSON via `makeSnapshotHandler(sampler)` (returns 503 with `Retry-After: 1` while warming up)
- `GET /events` → SSE stream via `makeEventsHandler(sampler)` (subscribes to the sampler, emits `ready` then `metrics` events; exits cleanly when the request context cancels or the subscription channel closes)

Neither handler does any sampling itself. The shared `Sampler.sample()` runs once per tick (1 Hz) and composes a `Snapshot` by calling subsystem samplers in `metrics.go` (CPU/mem/disk/net/processes/ports/git) and `ai.go` (AI agent counts, SSH remote sessions). It then calls `Sampler.updateEventsLocked` to advance the event ring before storing the snapshot atomically and broadcasting to subscribers.

### Cross-platform branching

Every metric sampler picks an implementation by `runtime.GOOS`:

- **Linux**: reads `/proc/stat`, `/proc/meminfo`, `/proc/net/dev`, `/proc/loadavg`, `/proc/uptime`, `/proc/cpuinfo`, `/etc/os-release` directly.
- **Darwin (and BSD-ish fallback)**: shells out via `util.go` `run()` to `ps`, `df`, `lsof`, `sysctl`, `vm_stat`, `netstat`, `sw_vers`-style tools.

When adding a new metric, follow the same pattern: a top-level function that branches on GOOS to a `linux*` or `darwin*`/`bsd*` helper. Subprocess calls always go through `run()` which applies a context timeout and returns `""` on any error (errors are silently swallowed — be aware when debugging missing data). The one exception is `Sampler.runDarwinTopStreamer`, which uses `exec.CommandContext` + `StdoutPipe` directly because it needs to stream a long-lived child rather than capture one-shot output.

### Shared sampler

`sampler.go` defines `Sampler`, instantiated once in `main.go` and run as a background goroutine. It owns:

- `previousCPU`, `previousNet`, `previousAt` — Linux delta-counter state, mutated only inside `Sampler.sample` under `s.mu`.
- `events []Event`, `eventID int64` — event ring (capped at 12, newest first), mutated by `updateEventsLocked`.
- `subscribers map[chan *Snapshot]*subState` — registered SSE clients.
- `latest atomic.Pointer[Snapshot]` — last produced snapshot, read lock-free by `/api/snapshot`.
- `darwinCPU atomic.Pointer[darwinCPU]` — instant CPU total parsed from a long-lived `top` child (Darwin only).
- `host hostCache` — facts that don't change at runtime (hostname, platform, arch, Go version, CPU model); computed once in `New()`.

`Run(ctx)` spawns the Darwin `top` streamer (if applicable), then ticks every second: `sample` → `latest.Store` → `broadcast`. On `ctx` cancellation it closes every subscriber channel and returns.

`Subscribe()` returns a cap-1 buffered channel and an unsubscribe func. `broadcast` does non-blocking sends; on full it drains and replaces (always newest), counts drops, and closes the channel after 3 consecutive drops. SSE handlers in `server.go` exit when their channel closes.

There is no per-client subprocess fan-out: every connected client sees the same `*Snapshot` produced by the shared sampler.

### Event ring semantics

`Sampler.updateEventsLocked` in `events.go` builds a list of `eventCandidate`s from threshold checks (CPU/memory/disk/hot-process), then `eventExistsIn` deduplicates by `(level, title, detail)` against the existing ring on the Sampler. Only new candidates get a new `ID` (monotonic) and are prepended. The ring is trimmed to 12. The frontend renders all 12 every tick — no incremental diff is sent.

### macOS CPU comes from `top`

On Darwin, `Sampler.runDarwinTopStreamer` runs `top -l 0 -s 1 -n 0` as a long-lived child process. Each second `top` prints a `CPU usage: X% user, Y% sys, Z% idle` line; `parseDarwinTopLine` extracts user+sys, the streamer stores it atomically, and `Sampler.cpuUsageLocked` reads it. The first emitted line (cumulative since boot) is discarded.

Per-core values on Darwin remain synthesized by `visualPerCore` because true per-core requires `cgo` + Mach `host_processor_info`. The `Snapshot.CPU.PerCoreEstimated` flag is set to `true` on Darwin; the frontend dims the blocks and appends an `(estimated)` label.

The Linux path is unchanged: `/proc/stat` deltas drive both total and per-core, and `PerCoreEstimated` is `false`.

### Frontend (`public/`)

`app.js` is a single ES module that opens one `EventSource("/events")` and on each `metrics` event calls `render(sample)`. Series arrays (`cpu`, `memory`, `network`, `load`) are FIFO-capped at 90 samples and drawn into `<canvas>` elements via `drawLineChart()`. The animated "core" canvas runs on its own `requestAnimationFrame` loop independent of the SSE tick. DOM lists (events, processes, AI, ports, disks) are rebuilt via `replaceChildren()` every tick — there is no diffing layer.

The HTML structure in `index.html` is a fixed grid of panels; the JS expects every `#id` listed in the `elements` map to exist in the DOM.

## Conventions

- Keep the backend zero-dep. The point of this project is "single binary, drop on a machine, run."
- Files are split by concern (`metrics.go`, `events.go`, `ai.go`, etc.). New samplers belong in their own file if they're more than a few helpers; reuse `util.go` for anything generic (timeout-bounded subprocess, JSON write, byte formatting).
- Type definitions live in `types.go`; don't scatter struct definitions across files. JSON tags are camelCase to match the frontend.
- Anything that calls a subprocess must use `run(timeout, ...)` — never `exec.Command(...).Output()` directly.
