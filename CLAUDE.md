# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`jsys` is a single-binary, zero-dependency Go service that samples the local machine and streams metrics to a browser UI ("Jerry's local system command center"). Backend module `github.com/jcdkuo/jsys`, Go 1.24. No external Go dependencies; the frontend is vanilla JS modules with no build step.

## Commands

```sh
go run .                        # run dev server on http://127.0.0.1:4173
PORT=5000 HOST=0.0.0.0 go run . # override bind address/port
go build -o jsys . && ./jsys    # produce single binary
go vet ./...                    # static analysis (no tests exist yet)
go fmt ./...                    # format
```

There are currently no tests. When adding the first test file, run a single test with `go test -run TestName ./...`.

## Architecture

### Request flow

`main.go` wires three routes on `http.ServeMux`:

- `GET /` → serves `public/` via `http.FileServer`
- `GET /api/snapshot` → one-shot JSON via `snapshotHandler`
- `GET /events` → SSE stream via `eventsHandler` (1Hz tick, emits `ready` / `metrics` / `error` events)

Both handlers call `sampleSystem()` in `snapshot.go`, which composes a `Snapshot` by calling subsystem samplers in `metrics.go` (CPU/mem/disk/net/processes/ports/git) and `ai.go` (AI agent counts, SSH remote sessions). The composed snapshot is then passed to `updateEvents()` in `events.go` which mutates the global event ring and returns the current event list.

### Cross-platform branching

Every metric sampler picks an implementation by `runtime.GOOS`:

- **Linux**: reads `/proc/stat`, `/proc/meminfo`, `/proc/net/dev`, `/proc/loadavg`, `/proc/uptime`, `/proc/cpuinfo`, `/etc/os-release` directly.
- **Darwin (and BSD-ish fallback)**: shells out via `util.go` `run()` to `ps`, `df`, `lsof`, `sysctl`, `vm_stat`, `netstat`, `sw_vers`-style tools.

When adding a new metric, follow the same pattern: a top-level function that branches on GOOS to a `linux*` or `darwin*`/`bsd*` helper. Subprocess calls always go through `run()` which applies a context timeout and returns `""` on any error (errors are silently swallowed — be aware when debugging missing data).

### Shared mutable sampling state

`state.go` defines a single global `state *appState` (declared in `types.go`) holding:

- `previousCPU []cpuTimes` — last `/proc/stat` snapshot for Linux delta CPU%
- `previousNet map[string]netCounters` — last per-interface byte counters for rate calc
- `previousAt time.Time` — timestamp of last net sample for interval division
- `events []Event` + `eventID int64` — event ring (capped at 12, newest first)

All four are protected by `state.mu`. **This is a process-global singleton, not per-client state.** Every call to `sampleSystem()` reads-and-replaces the previous-counter fields. With multiple concurrent SSE clients, deltas across clients race — keep this in mind before changing fan-out.

### Event ring semantics

`updateEvents()` in `events.go` builds a list of `eventCandidate`s from threshold checks (CPU/memory/disk/hot-process), then `eventExists()` deduplicates by `(level, title, detail)` against the existing ring. Only new candidates get a new `ID` (monotonic) and are prepended. The ring is trimmed to 12. The frontend renders all 12 every tick — no incremental diff is sent.

### macOS CPU is partially synthetic

`darwinCPUUsage()` sums `ps -A -o %cpu=` (cumulative process averages, not instant utilization), and `visualPerCore()` synthesizes per-core values from a sine wave. The Linux path via `/proc/stat` deltas is the real implementation. Don't trust macOS per-core blocks as ground truth.

### Frontend (`public/`)

`app.js` is a single ES module that opens one `EventSource("/events")` and on each `metrics` event calls `render(sample)`. Series arrays (`cpu`, `memory`, `network`, `load`) are FIFO-capped at 90 samples and drawn into `<canvas>` elements via `drawLineChart()`. The animated "core" canvas runs on its own `requestAnimationFrame` loop independent of the SSE tick. DOM lists (events, processes, AI, ports, disks) are rebuilt via `replaceChildren()` every tick — there is no diffing layer.

The HTML structure in `index.html` is a fixed grid of panels; the JS expects every `#id` listed in the `elements` map to exist in the DOM.

## Conventions

- Keep the backend zero-dep. The point of this project is "single binary, drop on a machine, run."
- Files are split by concern (`metrics.go`, `events.go`, `ai.go`, etc.). New samplers belong in their own file if they're more than a few helpers; reuse `util.go` for anything generic (timeout-bounded subprocess, JSON write, byte formatting).
- Type definitions live in `types.go`; don't scatter struct definitions across files. JSON tags are camelCase to match the frontend.
- Anything that calls a subprocess must use `run(timeout, ...)` — never `exec.Command(...).Output()` directly.
