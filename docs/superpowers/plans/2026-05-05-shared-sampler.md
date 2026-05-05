# Shared Sampler + macOS CPU Truthfulness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-client subprocess fan-out with a single shared sampler goroutine, fix multi-client races on the previous-counter state, replace synthetic macOS CPU total with real instant utilization from `top`, and add graceful shutdown.

**Architecture:** A new `Sampler` type owns sampling state (previous CPU/net counters, event ring), runs a 1 Hz ticker in a goroutine, and fan-outs each `Snapshot` to all SSE subscribers via cap-1 channels with replace-on-overflow + close-after-3-drops. `/api/snapshot` becomes a lock-free atomic-pointer read of the latest cached snapshot. macOS CPU total comes from a long-lived `top -l 0 -s 1 -n 0` child whose stdout is parsed into an atomic field; per-core stays synthetic but is flagged `PerCoreEstimated: true` and the UI labels it accordingly.

**Tech Stack:** Go 1.24 stdlib only (`net/http`, `os/exec`, `bufio`, `sync/atomic`, `signal`); vanilla JS frontend.

**Spec:** `docs/superpowers/specs/2026-05-05-shared-sampler-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `sampler.go` | create | `Sampler` type, `New`, `Run`, `Subscribe`, `broadcast`, `sample`, Linux/Darwin CPU & net helpers that touch state, Darwin `top` streamer, parser |
| `sampler_test.go` | create | Unit tests for subscribe/broadcast/parser; integration test for multi-client SSE |
| `state.go` | delete | Global `state` singleton goes away |
| `types.go` | modify | Remove `appState`; add `PerCoreEstimated bool` to `CPU`; add `subState` and `darwinCPU` types (consumed by sampler) |
| `snapshot.go` | delete | Body folds into `Sampler.sample` |
| `metrics.go` | modify | Drop `cpuUsage`/`linuxCPUUsage`/`networkUsage`/`darwinCPUUsage` wrappers that touched `state`; keep stateless helpers (`readLinuxCPUTimes`, `visualPerCore`, `memoryUsage`, `diskUsage`, `topProcesses`, `openPorts`, `gitStatus`, etc.) and the `bsdNetworkCounters`/`linuxNetworkCounters` raw readers |
| `events.go` | modify | `updateEvents(sample Snapshot)` becomes method `(s *Sampler) updateEventsLocked(snap *Snapshot) []Event`; `calculatePressure` stays a pure function |
| `server.go` | modify | Both handlers become closures over `*Sampler`; SSE handler uses `Subscribe` and exits on channel close |
| `main.go` | modify | Build `Sampler`, wire `signal.NotifyContext`, run sampler + server, do `server.Shutdown` on cancel |
| `public/app.js` | modify | `renderCores(cores, estimated)` accepts the flag; toggles `data-estimated` attr and `(estimated)` label |
| `public/styles.css` | modify | Add `.core-grid[data-estimated="true"] span { opacity: .6 }` rule |
| `CLAUDE.md` | modify | Replace "global mutable sampling state" + "macOS CPU is partially synthetic" sections with the new architecture |

---

## Task 1: Verify test harness

**Files:**
- Create: `sampler_test.go`

- [ ] **Step 1: Create empty test file**

```go
// sampler_test.go
package main

import "testing"

func TestSanity(t *testing.T) {
    if 1+1 != 2 {
        t.Fatal("math broken")
    }
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./...`
Expected: `ok  	github.com/jcdkuo/jsys`

- [ ] **Step 3: Commit**

```bash
git add sampler_test.go
git commit -m "Bootstrap test harness"
```

---

## Task 2: Sampler skeleton with Subscribe / unsubscribe

**Files:**
- Create: `sampler.go`
- Modify: `sampler_test.go`

- [ ] **Step 1: Write the failing tests**

Replace `sampler_test.go` contents with:

```go
package main

import (
    "testing"
    "time"
)

func TestSubscribeReturnsBufferedChannel(t *testing.T) {
    s := New()
    ch, _ := s.Subscribe()
    select {
    case <-ch:
        t.Fatal("expected channel to be empty initially")
    case <-time.After(10 * time.Millisecond):
    }
}

func TestUnsubscribeClosesChannel(t *testing.T) {
    s := New()
    ch, unsub := s.Subscribe()
    unsub()
    select {
    case _, ok := <-ch:
        if ok {
            t.Fatal("expected channel to be closed")
        }
    case <-time.After(50 * time.Millisecond):
        t.Fatal("channel did not close after unsubscribe")
    }
}

func TestUnsubscribeIsIdempotent(t *testing.T) {
    s := New()
    _, unsub := s.Subscribe()
    unsub()
    unsub() // must not panic
}

func TestMultipleSubscribersAreIndependent(t *testing.T) {
    s := New()
    a, _ := s.Subscribe()
    b, _ := s.Subscribe()
    if a == b {
        t.Fatal("expected distinct channels")
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run 'TestSubscribe|TestUnsubscribe|TestMultiple' ./...`
Expected: FAIL with `undefined: New`

- [ ] **Step 3: Create the Sampler skeleton**

Create `sampler.go`:

```go
package main

import (
    "sync"
    "sync/atomic"
    "time"
)

type Sampler struct {
    mu          sync.Mutex
    latest      atomic.Pointer[Snapshot]
    subscribers map[chan *Snapshot]*subState
    previousCPU []cpuTimes
    previousNet map[string]netCounters
    previousAt  time.Time
    eventID     int64
    events      []Event
    darwinCPU   atomic.Pointer[darwinCPU]
}

type subState struct {
    drops int
}

type darwinCPU struct {
    total float64
    at    time.Time
}

func New() *Sampler {
    return &Sampler{
        subscribers: map[chan *Snapshot]*subState{},
        previousNet: map[string]netCounters{},
        previousAt:  time.Now(),
    }
}

func (s *Sampler) Subscribe() (<-chan *Snapshot, func()) {
    ch := make(chan *Snapshot, 1)
    s.mu.Lock()
    s.subscribers[ch] = &subState{}
    s.mu.Unlock()
    return ch, func() { s.unsubscribe(ch) }
}

func (s *Sampler) unsubscribe(ch chan *Snapshot) {
    s.mu.Lock()
    if _, ok := s.subscribers[ch]; ok {
        delete(s.subscribers, ch)
        close(ch)
    }
    s.mu.Unlock()
}

func (s *Sampler) Latest() *Snapshot {
    return s.latest.Load()
}
```

Add to `types.go` (alongside existing types — do NOT remove `appState` yet, that comes later):

```go
// (no edits required for this task — subState and darwinCPU live in sampler.go)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run 'TestSubscribe|TestUnsubscribe|TestMultiple' ./...`
Expected: PASS

Run: `go vet ./...`
Expected: clean

- [ ] **Step 5: Commit**

```bash
git add sampler.go sampler_test.go
git commit -m "Add Sampler skeleton with Subscribe lifecycle"
```

---

## Task 3: Broadcast with slow-consumer policy

**Files:**
- Modify: `sampler.go`, `sampler_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `sampler_test.go`:

```go
func TestBroadcastDeliversToReadyConsumer(t *testing.T) {
    s := New()
    ch, _ := s.Subscribe()
    snap := &Snapshot{Timestamp: 42}

    s.broadcast(snap)

    select {
    case got := <-ch:
        if got.Timestamp != 42 {
            t.Fatalf("got %d, want 42", got.Timestamp)
        }
    case <-time.After(50 * time.Millisecond):
        t.Fatal("expected snapshot on channel")
    }
}

func TestBroadcastReplacesStaleSnapshotForSlowConsumer(t *testing.T) {
    s := New()
    ch, _ := s.Subscribe()

    s.broadcast(&Snapshot{Timestamp: 1})
    s.broadcast(&Snapshot{Timestamp: 2})

    got := <-ch
    if got.Timestamp != 2 {
        t.Fatalf("expected newest snapshot (2), got %d", got.Timestamp)
    }
}

func TestBroadcastClosesAfterThreeConsecutiveDrops(t *testing.T) {
    s := New()
    ch, _ := s.Subscribe()

    s.broadcast(&Snapshot{Timestamp: 1}) // delivered, drops=0
    s.broadcast(&Snapshot{Timestamp: 2}) // replaces, drops=1
    s.broadcast(&Snapshot{Timestamp: 3}) // replaces, drops=2
    s.broadcast(&Snapshot{Timestamp: 4}) // replaces, drops=3 → close

    deadline := time.After(100 * time.Millisecond)
    for {
        select {
        case _, ok := <-ch:
            if !ok {
                return
            }
        case <-deadline:
            t.Fatal("expected channel to close after 3 consecutive drops")
        }
    }
}

func TestBroadcastResetsDropCountAfterSuccessfulDelivery(t *testing.T) {
    s := New()
    ch, _ := s.Subscribe()

    s.broadcast(&Snapshot{Timestamp: 1}) // drops=0, buffered
    s.broadcast(&Snapshot{Timestamp: 2}) // drops=1
    <-ch                                  // drain
    s.broadcast(&Snapshot{Timestamp: 3}) // delivered, drops=0
    s.broadcast(&Snapshot{Timestamp: 4}) // drops=1
    s.broadcast(&Snapshot{Timestamp: 5}) // drops=2

    // channel should still be open
    select {
    case got, ok := <-ch:
        if !ok {
            t.Fatal("channel closed prematurely")
        }
        if got.Timestamp != 5 {
            t.Fatalf("expected newest (5), got %d", got.Timestamp)
        }
    case <-time.After(50 * time.Millisecond):
        t.Fatal("expected snapshot on channel")
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestBroadcast ./...`
Expected: FAIL with `s.broadcast undefined`

- [ ] **Step 3: Implement broadcast**

Append to `sampler.go`:

```go
func (s *Sampler) broadcast(snap *Snapshot) {
    s.mu.Lock()
    defer s.mu.Unlock()
    for ch, st := range s.subscribers {
        select {
        case ch <- snap:
            st.drops = 0
        default:
            select {
            case <-ch:
            default:
            }
            select {
            case ch <- snap:
            default:
            }
            st.drops++
            if st.drops >= 3 {
                delete(s.subscribers, ch)
                close(ch)
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestBroadcast ./...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sampler.go sampler_test.go
git commit -m "Add Sampler broadcast with slow-consumer policy"
```

---

## Task 4: Add `PerCoreEstimated` field to CPU

**Files:**
- Modify: `types.go`

- [ ] **Step 1: Add the field**

In `types.go`, change the `CPU` struct:

```go
type CPU struct {
    Total            float64   `json:"total"`
    PerCore          []float64 `json:"perCore"`
    PerCoreEstimated bool      `json:"perCoreEstimated"`
    Cores            int       `json:"cores"`
    Model            string    `json:"model"`
    LoadAverage      []float64 `json:"loadAverage"`
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./...`
Expected: clean

Run: `go test ./...`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add types.go
git commit -m "Add CPU.PerCoreEstimated field"
```

---

## Task 5: Move sampling logic onto `Sampler.sample`

This task is the biggest single refactor: it moves all per-tick sampling onto the `Sampler` value, removes the global `state`, and drops `snapshot.go` and `state.go`. There is no clean intermediate compile state, so we batch the structural change and run `go build` + `go test` at the end.

**Files:**
- Modify: `sampler.go`, `metrics.go`, `events.go`, `types.go`
- Delete: `state.go`, `snapshot.go`

- [ ] **Step 1: Move the previous-counter helpers onto Sampler**

In `metrics.go`, **delete** the existing `cpuUsage`, `linuxCPUUsage`, `networkUsage`, and the package-level state references in their bodies. Keep `readLinuxCPUTimes`, `darwinCPUUsage`, `visualPerCore`, `linuxNetworkCounters`, `bsdNetworkCounters`, and every other stateless helper.

In `sampler.go`, append:

```go
func (s *Sampler) cpuUsageLocked() CPU {
    cores := runtimeNumCPU()
    total := 0.0
    perCore := []float64{}
    estimated := false

    if isLinux() {
        perCore = s.linuxCPUUsageLocked()
        if len(perCore) > 0 {
            for _, v := range perCore {
                total += v
            }
            total /= float64(len(perCore))
            cores = len(perCore)
        }
    } else {
        if dc := s.darwinCPU.Load(); dc != nil {
            total = dc.total
        } else {
            total = loadFallback()
        }
        perCore = visualPerCore(total, cores)
        estimated = true
    }

    return CPU{
        Total:            clamp(total, 0, 100),
        PerCore:          perCore,
        PerCoreEstimated: estimated,
        Cores:            cores,
        Model:            s.host.cpuModel,
        LoadAverage:      loadAverage(),
    }
}

func (s *Sampler) linuxCPUUsageLocked() []float64 {
    current := readLinuxCPUTimes()
    if len(current) == 0 {
        return nil
    }
    before := s.previousCPU
    s.previousCPU = current
    if len(before) != len(current) {
        return visualPerCore(loadFallback(), len(current))
    }
    values := make([]float64, 0, len(current))
    for i, now := range current {
        old := before[i]
        idle := float64(now.idle - old.idle)
        total := float64(sumCPU(now) - sumCPU(old))
        v := 0.0
        if total > 0 {
            v = ((total - idle) / total) * 100
        }
        values = append(values, clamp(v, 0, 100))
    }
    return values
}

func (s *Sampler) networkUsageLocked(now time.Time) Network {
    current := networkCounters()
    interval := now.Sub(s.previousAt).Seconds()
    if interval < 0.25 {
        interval = 0.25
    }
    before := s.previousNet
    s.previousNet = current
    s.previousAt = now

    var interfaces []NetInterface
    for name, stats := range current {
        if strings.HasPrefix(name, "lo") {
            continue
        }
        old, ok := before[name]
        if !ok {
            old = stats
        }
        rxRate := math.Max(float64(stats.rxBytes-old.rxBytes)/interval, 0)
        txRate := math.Max(float64(stats.txBytes-old.txBytes)/interval, 0)
        interfaces = append(interfaces, NetInterface{
            Name: name, RxBytes: stats.rxBytes, TxBytes: stats.txBytes, RxRate: rxRate, TxRate: txRate,
        })
    }
    sort.Slice(interfaces, func(i, j int) bool {
        return interfaces[i].RxRate+interfaces[i].TxRate > interfaces[j].RxRate+interfaces[j].TxRate
    })
    if len(interfaces) > 5 {
        interfaces = interfaces[:5]
    }
    network := Network{Interfaces: interfaces}
    for _, item := range interfaces {
        network.RxRate += item.RxRate
        network.TxRate += item.TxRate
    }
    return network
}
```

Required imports in `sampler.go` after this step: `math`, `runtime`, `sort`, `strings`, `sync`, `sync/atomic`, `time`. (`bufio`, `context`, `os/exec`, `regexp` are added in later tasks.)

Add small helpers in `sampler.go`:

```go
func runtimeNumCPU() int { return runtime.NumCPU() }
func isLinux() bool      { return runtime.GOOS == "linux" }
```

Cache the static host fields. Add a new `hostCache` type and a `host` field on `Sampler`:

```go
type hostCache struct {
    name     string
    platform string
    arch     string
    nodeVer  string
    cpuModel string
}
```

Add `host hostCache` as a field on the `Sampler` struct (insert it just below `subscribers`).

Update `New` to populate it once:

```go
func New() *Sampler {
    s := &Sampler{
        subscribers: map[chan *Snapshot]*subState{},
        previousNet: map[string]netCounters{},
        previousAt:  time.Now(),
    }
    s.host = hostCache{
        name:     hostname(),
        platform: platformName(),
        arch:     runtime.GOARCH,
        nodeVer:  runtime.Version(),
        cpuModel: cpuModel(),
    }
    return s
}
```

- [ ] **Step 2: Add `Sampler.sample`**

Append to `sampler.go`:

```go
func (s *Sampler) sample() *Snapshot {
    now := time.Now()

    s.mu.Lock()
    cpu := s.cpuUsageLocked()
    network := s.networkUsageLocked(now)
    s.mu.Unlock()

    memory := memoryUsage()
    disks := diskUsage()
    processes := topProcesses()
    ports := openPorts()
    git := gitStatus()
    ai := aiStatus()

    primaryDisk := Disk{}
    if len(disks) > 0 {
        primaryDisk = disks[0]
        for _, d := range disks {
            if d.Mount == "/" {
                primaryDisk = d
                break
            }
        }
    }
    health := calculatePressure(cpu, memory, primaryDisk, network)

    snap := &Snapshot{
        Timestamp: now.UnixMilli(),
        Host: Host{
            Name:     s.host.name,
            Platform: s.host.platform,
            Arch:     s.host.arch,
            Uptime:   uptimeSeconds(),
            Node:     s.host.nodeVer,
        },
        Health:    health,
        AI:        ai,
        CPU:       cpu,
        Memory:    memory,
        Disks:     disks,
        Network:   network,
        Processes: processes,
        Ports:     ports,
        Git:       git,
    }

    s.mu.Lock()
    snap.Events = s.updateEventsLocked(snap)
    s.mu.Unlock()

    return snap
}
```

- [ ] **Step 3: Convert `updateEvents` to method**

Replace the body of `events.go` with:

```go
package main

import (
    "fmt"
    "math"
    "time"
)

func calculatePressure(cpu CPU, memory Memory, disk Disk, network Network) Health {
    loadPressure := 0.0
    if cpu.Cores > 0 && len(cpu.LoadAverage) > 0 {
        loadPressure = clamp((cpu.LoadAverage[0]/float64(cpu.Cores))*100, 0, 100)
    }
    networkPressure := clamp(math.Log10(1+network.RxRate+network.TxRate)*11, 0, 100)
    score := clamp(cpu.Total*0.34+memory.Percent*0.28+disk.Percent*0.20+loadPressure*0.14+networkPressure*0.04, 0, 100)
    state := "Stable"
    if score >= 82 {
        state = "Critical"
    } else if score >= 62 {
        state = "Pressure"
    }
    return Health{Score: score, State: state}
}

func (s *Sampler) updateEventsLocked(snap *Snapshot) []Event {
    var candidates []eventCandidate
    if snap.CPU.Total > 85 {
        candidates = append(candidates, eventCandidate{"critical", "CPU saturation", fmt.Sprintf("%.0f%% utilization across %d cores", snap.CPU.Total, snap.CPU.Cores)})
    } else if snap.CPU.Total > 65 {
        candidates = append(candidates, eventCandidate{"warn", "CPU pressure", fmt.Sprintf("%.0f%% utilization trend detected", snap.CPU.Total)})
    }

    if snap.Memory.Percent > 88 {
        candidates = append(candidates, eventCandidate{"critical", "Memory ceiling", fmt.Sprintf("%s used of %s", formatBytes(snap.Memory.Used), formatBytes(snap.Memory.Total))})
    } else if snap.Memory.Percent > 72 {
        candidates = append(candidates, eventCandidate{"warn", "Memory pressure", fmt.Sprintf("%.0f%% RAM allocation", snap.Memory.Percent)})
    }

    for _, disk := range snap.Disks {
        if disk.Percent <= 80 {
            continue
        }
        level := "warn"
        if disk.Percent > 90 {
            level = "critical"
        }
        candidates = append(candidates, eventCandidate{level, "Storage threshold", fmt.Sprintf("%s is %.0f%% full", disk.Mount, disk.Percent)})
        if len(candidates) >= 4 {
            break
        }
    }

    if len(snap.Processes) > 0 && snap.Processes[0].CPU > 60 {
        top := snap.Processes[0]
        candidates = append(candidates, eventCandidate{"warn", "Hot process", fmt.Sprintf("%s is using %.0f%% CPU", top.Command, top.CPU)})
    }

    if len(candidates) == 0 && len(s.events) == 0 {
        candidates = append(candidates, eventCandidate{"info", "Telemetry online", "Live system stream established"})
    }

    now := time.Now().UnixMilli()
    for _, candidate := range candidates {
        if eventExistsIn(s.events, candidate) {
            continue
        }
        s.eventID++
        event := Event{ID: s.eventID, Level: candidate.level, Title: candidate.title, Detail: candidate.detail, Time: now}
        s.events = append([]Event{event}, s.events...)
    }
    if len(s.events) > 12 {
        s.events = s.events[:12]
    }

    out := make([]Event, len(s.events))
    copy(out, s.events)
    return out
}

func eventExistsIn(events []Event, candidate eventCandidate) bool {
    for _, event := range events {
        if event.Level == candidate.level && event.Title == candidate.title && event.Detail == candidate.detail {
            return true
        }
    }
    return false
}
```

- [ ] **Step 4: Delete `state.go` and `snapshot.go`**

Run:

```bash
rm state.go snapshot.go
```

- [ ] **Step 5: Remove `appState` from `types.go`**

In `types.go`, delete the `appState` struct definition entirely. The `cpuTimes`, `netCounters`, `eventCandidate`, and `procInfo` types remain.

- [ ] **Step 6: Verify build and tests**

Run: `go build ./...`
Expected: errors about `state` references in `server.go` (handled in Task 6) — that's fine, but we need this task to compile first. Read the errors; they should ONLY be about `server.go` referencing `sampleSystem` and `state`. If any error is in `metrics.go`, `events.go`, `sampler.go`, or `types.go`, fix before proceeding.

If `server.go` is the only file with errors, **temporarily** stub it so the package compiles for testing:

In `server.go`, replace the entire file with:

```go
package main

// handlers are wired in main.go after the Sampler refactor; see Task 6.
```

Then in `main.go`, comment out the route registrations referencing the old handlers (we'll fully rewrite both in Task 6):

```go
// mux.HandleFunc("/api/snapshot", snapshotHandler)
// mux.HandleFunc("/events", eventsHandler)
```

Run: `go build ./...`
Expected: clean

Run: `go test ./...`
Expected: PASS (existing Subscribe/broadcast tests still pass)

- [ ] **Step 7: Commit**

```bash
git add sampler.go metrics.go events.go types.go server.go main.go
git rm state.go snapshot.go
git commit -m "Move sampling onto Sampler; drop global state"
```

---

## Task 6: Sampler.Run, handler wiring, and graceful shutdown

**Files:**
- Modify: `sampler.go`, `sampler_test.go`, `server.go`, `main.go`

- [ ] **Step 1: Write the failing tests**

Append to `sampler_test.go` (merge `context` into the file's existing `import` block):

```go
func TestRunCancelExitsCleanly(t *testing.T) {
    s := New()
    ctx, cancel := context.WithCancel(context.Background())
    done := make(chan struct{})
    go func() {
        _ = s.runForTest(ctx, 20*time.Millisecond)
        close(done)
    }()
    time.Sleep(80 * time.Millisecond)
    cancel()
    select {
    case <-done:
    case <-time.After(500 * time.Millisecond):
        t.Fatal("Run did not exit after context cancel")
    }
}

func TestRunPopulatesLatest(t *testing.T) {
    s := New()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    go s.runForTest(ctx, 20*time.Millisecond)

    deadline := time.Now().Add(500 * time.Millisecond)
    for time.Now().Before(deadline) {
        if s.Latest() != nil {
            return
        }
        time.Sleep(20 * time.Millisecond)
    }
    t.Fatal("Latest() never populated")
}

func TestRunClosesSubscribersOnShutdown(t *testing.T) {
    s := New()
    ctx, cancel := context.WithCancel(context.Background())
    go s.runForTest(ctx, 20*time.Millisecond)

    ch, _ := s.Subscribe()
    cancel()
    select {
    case _, ok := <-ch:
        if ok {
            // first read may yield a snapshot; drain and try again
            select {
            case _, ok2 := <-ch:
                if ok2 {
                    t.Fatal("expected channel to close after shutdown")
                }
            case <-time.After(200 * time.Millisecond):
                t.Fatal("channel not closed after shutdown")
            }
        }
    case <-time.After(200 * time.Millisecond):
        t.Fatal("channel not closed after shutdown")
    }
}
```

Also append (this is the multi-client integration test the spec calls for). Merge these imports into the existing `import` block at the top of `sampler_test.go`: `bufio`, `context`, `net/http`, `net/http/httptest`, `strings`.

```go
func TestTwoSSEClientsReceiveSameSnapshots(t *testing.T) {
    s := New()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    go s.runForTest(ctx, 50*time.Millisecond)

    mux := http.NewServeMux()
    mux.HandleFunc("/events", makeEventsHandler(s))
    srv := httptest.NewServer(mux)
    defer srv.Close()

    readN := func(n int) []string {
        resp, err := http.Get(srv.URL + "/events")
        if err != nil {
            t.Fatal(err)
        }
        t.Cleanup(func() { resp.Body.Close() })
        scanner := bufio.NewScanner(resp.Body)
        var lines []string
        for scanner.Scan() && len(lines) < n {
            line := scanner.Text()
            if strings.HasPrefix(line, "data: ") {
                lines = append(lines, line)
            }
        }
        return lines
    }

    a := make(chan []string, 1)
    b := make(chan []string, 1)
    go func() { a <- readN(3) }()
    go func() { b <- readN(3) }()

    select {
    case linesA := <-a:
        linesB := <-b
        if len(linesA) != 3 || len(linesB) != 3 {
            t.Fatalf("expected 3 data lines each, got %d / %d", len(linesA), len(linesB))
        }
    case <-time.After(2 * time.Second):
        t.Fatal("timeout waiting for SSE data")
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run 'TestRun|TestTwo' ./...`
Expected: FAIL with `s.runForTest undefined` and `makeEventsHandler undefined`

- [ ] **Step 3: Implement Run and runForTest**

Append to `sampler.go` (add `context` to the existing `import` block):

```go
func (s *Sampler) Run(ctx context.Context) {
    s.runForTest(ctx, time.Second)
}

func (s *Sampler) runForTest(ctx context.Context, interval time.Duration) error {
    if !isLinux() {
        go s.runDarwinTopStreamer(ctx)
    }
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    snap := s.sample()
    s.latest.Store(snap)
    s.broadcast(snap)

    for {
        select {
        case <-ctx.Done():
            s.closeAllSubscribers()
            return nil
        case <-ticker.C:
            snap := s.sample()
            s.latest.Store(snap)
            s.broadcast(snap)
        }
    }
}

func (s *Sampler) closeAllSubscribers() {
    s.mu.Lock()
    defer s.mu.Unlock()
    for ch := range s.subscribers {
        close(ch)
    }
    s.subscribers = map[chan *Snapshot]*subState{}
}

// runDarwinTopStreamer is implemented in Task 7. Stub for now:
func (s *Sampler) runDarwinTopStreamer(ctx context.Context) {}
```

(`context` and the existing imports merge in sampler.go.)

- [ ] **Step 4: Wire handlers**

Replace `server.go` with:

```go
package main

import (
    "net/http"
)

func makeSnapshotHandler(s *Sampler) http.HandlerFunc {
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

func makeEventsHandler(s *Sampler) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "text/event-stream")
        w.Header().Set("Cache-Control", "no-cache")
        w.Header().Set("Connection", "keep-alive")
        w.Header().Set("X-Accel-Buffering", "no")

        flusher, ok := w.(http.Flusher)
        if !ok {
            http.Error(w, "streaming unsupported", http.StatusInternalServerError)
            return
        }

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
```

- [ ] **Step 5: Wire main with graceful shutdown**

Replace `main.go` with:

```go
package main

import (
    "context"
    "log"
    "net/http"
    "os/signal"
    "syscall"
    "time"
)

func main() {
    host := env("HOST", "127.0.0.1")
    port := env("PORT", "4173")
    addr := host + ":" + port

    ctx, stop := signal.NotifyContext(context.Background(),
        syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    sampler := New()
    samplerDone := make(chan struct{})
    go func() {
        sampler.Run(ctx)
        close(samplerDone)
    }()

    mux := http.NewServeMux()
    mux.HandleFunc("/api/snapshot", makeSnapshotHandler(sampler))
    mux.HandleFunc("/events", makeEventsHandler(sampler))
    mux.Handle("/", http.FileServer(http.Dir("public")))

    server := &http.Server{Addr: addr, Handler: mux}

    go func() {
        <-ctx.Done()
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        _ = server.Shutdown(shutdownCtx)
    }()

    log.Printf("jsys command center: http://%s", addr)
    if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        log.Fatal(err)
    }
    <-samplerDone
}
```

- [ ] **Step 6: Run tests**

Run: `go test ./...`
Expected: PASS

Run: `go vet ./...`
Expected: clean

- [ ] **Step 7: Smoke test the binary manually**

Run: `go run .` and in another terminal: `curl -N http://127.0.0.1:4173/events | head -20`. Verify SSE frames appear, then `Ctrl-C` the server. Should exit within a second cleanly (no `panic` or `goroutine` dump).

- [ ] **Step 8: Commit**

```bash
git add sampler.go sampler_test.go server.go main.go
git commit -m "Run sampler as background goroutine with graceful shutdown"
```

---

## Task 7: Darwin `top` streamer with parser test

**Files:**
- Modify: `sampler.go`, `sampler_test.go`

- [ ] **Step 1: Write the failing parser test**

Append to `sampler_test.go`:

```go
func TestParseDarwinTopLine(t *testing.T) {
    cases := []struct {
        name    string
        line    string
        wantOK  bool
        wantTot float64
    }{
        {"happy", "CPU usage: 4.12% user, 2.88% sys, 93.00% idle", true, 7.00},
        {"high",  "CPU usage: 78.50% user, 12.50% sys, 9.00% idle", true, 91.00},
        {"zero",  "CPU usage: 0.00% user, 0.00% sys, 100.00% idle", true, 0.00},
        {"miss",  "Processes: 612 total, 3 running", false, 0},
        {"junk",  "", false, 0},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            got, ok := parseDarwinTopLine(tc.line)
            if ok != tc.wantOK {
                t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
            }
            if ok && got != tc.wantTot {
                t.Fatalf("got %.2f, want %.2f", got, tc.wantTot)
            }
        })
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run TestParseDarwinTopLine ./...`
Expected: FAIL with `parseDarwinTopLine undefined`

- [ ] **Step 3: Implement parser and streamer**

In `sampler.go`, replace the `runDarwinTopStreamer` stub with the real implementation and add the parser:

```go
var darwinTopRegex = regexp.MustCompile(`CPU usage:\s*([\d.]+)%\s*user,\s*([\d.]+)%\s*sys`)

func parseDarwinTopLine(line string) (float64, bool) {
    match := darwinTopRegex.FindStringSubmatch(line)
    if len(match) != 3 {
        return 0, false
    }
    total := parseFloat(match[1]) + parseFloat(match[2])
    return clamp(total, 0, 100), true
}

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
    firstSeen := false
    for scanner.Scan() {
        total, ok := parseDarwinTopLine(scanner.Text())
        if !ok {
            continue
        }
        if !firstSeen {
            firstSeen = true
            continue // discard "since boot" cumulative
        }
        s.darwinCPU.Store(&darwinCPU{total: total, at: time.Now()})
    }
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./...`
Expected: PASS

- [ ] **Step 5: Manual verification on macOS**

Run: `go run .`. Open `http://127.0.0.1:4173`. Observe the CPU number for 10 seconds and confirm it tracks real load (e.g., spike a core with `yes > /dev/null & sleep 5; kill %1` and watch the number rise then fall). The previous `ps`-summed implementation moved very slowly; the new value should respond within 1-2 ticks.

- [ ] **Step 6: Commit**

```bash
git add sampler.go sampler_test.go
git commit -m "Stream macOS CPU from long-lived top child"
```

---

## Task 8: Frontend — render `(estimated)` label and dim per-core blocks

**Files:**
- Modify: `public/app.js`, `public/styles.css`

- [ ] **Step 1: Update renderCores signature and call site in `app.js`**

Find the `renderCores` function (around `app.js:167`) and replace with:

```js
function renderCores(cores, estimated) {
  elements.coreGrid.dataset.estimated = estimated ? "true" : "false";

  const cpuHeader = document.querySelector('article[data-metric="cpu"] .metric-card__header');
  const existingTag = cpuHeader.querySelector(".estimated-tag");
  if (estimated && !existingTag) {
    const tag = document.createElement("small");
    tag.className = "estimated-tag";
    tag.textContent = "(estimated)";
    cpuHeader.appendChild(tag);
  } else if (!estimated && existingTag) {
    existingTag.remove();
  }

  elements.coreGrid.replaceChildren(
    ...cores.map((value) => {
      const node = document.createElement("span");
      node.title = `${value.toFixed(1)}%`;
      node.style.background = `linear-gradient(to top, ${heatColor(value)} ${value}%, rgba(255,255,255,0.08) ${value}%)`;
      return node;
    })
  );
}
```

In the `render(sample)` function (around `app.js:107`), update the call:

```js
renderCores(sample.cpu.perCore, !!sample.cpu.perCoreEstimated);
```

- [ ] **Step 2: Add styling rule in `styles.css`**

Append to `public/styles.css`:

```css
.core-grid[data-estimated="true"] span {
  opacity: 0.6;
}

.metric-card__header .estimated-tag {
  margin-left: 0.5rem;
  font-size: 0.7rem;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.45);
}
```

- [ ] **Step 3: Manual verification**

Run: `go run .` on macOS. Open `http://127.0.0.1:4173`. The CPU card header should now read `CPU --% (estimated)` and the per-core blocks should be visibly dimmer than the line chart above them.

If you have access to a Linux box, run there and confirm `(estimated)` does NOT appear and per-core blocks render at full opacity.

- [ ] **Step 4: Commit**

```bash
git add public/app.js public/styles.css
git commit -m "Mark macOS per-core CPU as estimated in UI"
```

---

## Task 9: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Replace the architecture sections**

Open `CLAUDE.md`. Replace the entire `### Shared mutable sampling state` section with:

```markdown
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
```

Replace the entire `### macOS CPU is partially synthetic` section with:

```markdown
### macOS CPU comes from `top`

On Darwin, `Sampler.runDarwinTopStreamer` runs `top -l 0 -s 1 -n 0` as a long-lived child process. Each second `top` prints a `CPU usage: X% user, Y% sys, Z% idle` line; `parseDarwinTopLine` extracts user+sys, the streamer stores it atomically, and `Sampler.cpuUsageLocked` reads it. The first emitted line (cumulative since boot) is discarded.

Per-core values on Darwin remain synthesized by `visualPerCore` because true per-core requires `cgo` + Mach `host_processor_info`. The `Snapshot.CPU.PerCoreEstimated` flag is set to `true` on Darwin; the frontend dims the blocks and appends an `(estimated)` label.

The Linux path is unchanged: `/proc/stat` deltas drive both total and per-core, and `PerCoreEstimated` is `false`.
```

If the Migration plan section in `CLAUDE.md` references `state.go` or the global `state`, remove those mentions.

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "Update CLAUDE.md for shared-sampler architecture"
```

---

## Self-Review

Spec coverage:

- ✅ Shared sampler producing one snapshot for all clients → Tasks 2, 5, 6
- ✅ Slow-consumer policy (replace + close after 3) → Task 3
- ✅ macOS CPU instant total via `top -l 0` → Task 7
- ✅ `PerCoreEstimated` flag → Task 4 + Task 5 (sampler sets it) + Task 8 (frontend)
- ✅ `/api/snapshot` returns 503 during warmup → Task 6
- ✅ Static host fields cached once → Task 5
- ✅ Graceful shutdown via `signal.NotifyContext` + `server.Shutdown` → Task 6
- ✅ `state.go` deleted; `appState` removed → Task 5
- ✅ Multi-client integration test → Task 6
- ✅ Frontend dim + label → Task 8
- ✅ CLAUDE.md updated → Task 9

Type / signature consistency:
- `Sampler` fields introduced in Task 2 (`host hostCache` deferred to Task 5 — Task 5 explicitly replaces the field).
- `New()` signature `func New() *Sampler` consistent across Tasks 2 and 5.
- `Subscribe() (<-chan *Snapshot, func())` consistent in Tasks 2, 6.
- `broadcast(snap *Snapshot)` consistent.
- `Sampler.Run(ctx context.Context)` and helper `runForTest(ctx, interval)` consistent.
- Handler factory names consistent: `makeSnapshotHandler`, `makeEventsHandler` (Task 6).
- `parseDarwinTopLine(line string) (float64, bool)` consistent between Task 7 test and impl.

No placeholders, no TBDs, no "similar to Task N" — every step contains the literal code or command needed.
