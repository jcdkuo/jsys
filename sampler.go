package main

import (
	"bufio"
	"context"
	"log"
	"math"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type hostCache struct {
	name     string
	platform string
	arch     string
	nodeVer  string
	cpuModel string
}

type Sampler struct {
	mu          sync.Mutex
	latest      atomic.Pointer[Snapshot]
	subscribers map[chan *Snapshot]*subState
	host        hostCache
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

func runtimeNumCPU() int { return runtime.NumCPU() }
func isLinux() bool      { return runtime.GOOS == "linux" }

func (s *Sampler) cpuUsageLocked(rawLinuxCPU []cpuTimes) CPU {
	cores := runtimeNumCPU()
	total := 0.0
	perCore := []float64{}
	estimated := false

	if isLinux() {
		perCore = s.linuxCPUUsageLocked(rawLinuxCPU)
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

func (s *Sampler) linuxCPUUsageLocked(current []cpuTimes) []float64 {
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

func (s *Sampler) networkUsageLocked(now time.Time, current map[string]netCounters) Network {
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

func (s *Sampler) sample() *Snapshot {
	now := time.Now()

	// I/O outside the lock — Darwin netstat can take up to 1200 ms.
	var rawCPU []cpuTimes
	if isLinux() {
		rawCPU = readLinuxCPUTimes()
	}
	rawNet := networkCounters()

	s.mu.Lock()
	cpu := s.cpuUsageLocked(rawCPU)
	network := s.networkUsageLocked(now, rawNet)
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

func (s *Sampler) Run(ctx context.Context) {
	s.runForTest(ctx, time.Second)
}

func (s *Sampler) runForTest(ctx context.Context, interval time.Duration) error {
	if !isLinux() {
		go s.runDarwinTopStreamer(ctx)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.closeAllSubscribers()
			return nil
		case <-ticker.C:
			if ctx.Err() != nil {
				s.closeAllSubscribers()
				return nil
			}
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
	defer s.darwinCPU.Store(nil)

	cmd := exec.CommandContext(ctx, "top", "-l", "0", "-s", "1", "-n", "0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("top streamer stdout pipe: %v", err)
		return
	}
	if err := cmd.Start(); err != nil {
		log.Printf("top streamer start: %v", err)
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
