package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type appState struct {
	mu          sync.Mutex
	previousCPU []cpuTimes
	previousNet map[string]netCounters
	previousAt  time.Time
	eventID     int64
	events      []Event
}

type Snapshot struct {
	Timestamp int64         `json:"timestamp"`
	Host      Host          `json:"host"`
	Health    Health        `json:"health"`
	CPU       CPU           `json:"cpu"`
	Memory    Memory        `json:"memory"`
	Disks     []Disk        `json:"disks"`
	Network   Network       `json:"network"`
	Processes []ProcessInfo `json:"processes"`
	Ports     []int         `json:"ports"`
	Git       GitStatus     `json:"git"`
	Events    []Event       `json:"events"`
}

type Host struct {
	Name     string  `json:"name"`
	Platform string  `json:"platform"`
	Arch     string  `json:"arch"`
	Uptime   float64 `json:"uptime"`
	Node     string  `json:"node"`
}

type Health struct {
	Score float64 `json:"score"`
	State string  `json:"state"`
}

type CPU struct {
	Total       float64   `json:"total"`
	PerCore     []float64 `json:"perCore"`
	Cores       int       `json:"cores"`
	Model       string    `json:"model"`
	LoadAverage []float64 `json:"loadAverage"`
}

type Memory struct {
	Total   uint64  `json:"total"`
	Used    uint64  `json:"used"`
	Free    uint64  `json:"free"`
	Percent float64 `json:"percent"`
	Swap    Swap    `json:"swap"`
}

type Swap struct {
	Total   uint64  `json:"total"`
	Used    uint64  `json:"used"`
	Percent float64 `json:"percent"`
}

type Disk struct {
	Filesystem string  `json:"filesystem"`
	Size       uint64  `json:"size"`
	Used       uint64  `json:"used"`
	Available  uint64  `json:"available"`
	Percent    float64 `json:"percent"`
	Mount      string  `json:"mount"`
}

type Network struct {
	RxRate     float64        `json:"rxRate"`
	TxRate     float64        `json:"txRate"`
	Interfaces []NetInterface `json:"interfaces"`
}

type NetInterface struct {
	Name    string  `json:"name"`
	RxBytes uint64  `json:"rxBytes"`
	TxBytes uint64  `json:"txBytes"`
	RxRate  float64 `json:"rxRate"`
	TxRate  float64 `json:"txRate"`
}

type ProcessInfo struct {
	PID     int     `json:"pid"`
	Command string  `json:"command"`
	CPU     float64 `json:"cpu"`
	Memory  float64 `json:"memory"`
}

type GitStatus struct {
	Branch       string `json:"branch"`
	ChangedFiles int    `json:"changedFiles"`
	Clean        bool   `json:"clean"`
}

type Event struct {
	ID     int64  `json:"id"`
	Level  string `json:"level"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Time   int64  `json:"time"`
}

type cpuTimes struct {
	user uint64
	nice uint64
	sys  uint64
	idle uint64
	wait uint64
	irq  uint64
	soft uint64
}

type netCounters struct {
	rxBytes uint64
	txBytes uint64
}

type eventCandidate struct {
	level  string
	title  string
	detail string
}

var (
	state = &appState{
		previousCPU: readLinuxCPUTimes(),
		previousNet: map[string]netCounters{},
		previousAt:  time.Now(),
	}
	processPattern = regexp.MustCompile(`^\s*(\d+)\s+(.+?)\s+([\d.]+)\s+([\d.]+)\s*$`)
	portPattern    = regexp.MustCompile(`[:.]([0-9]+)([[:space:]]|\)|$)`)
)

func main() {
	host := env("HOST", "127.0.0.1")
	port := env("PORT", "4173")
	addr := host + ":" + port

	mux := http.NewServeMux()
	mux.HandleFunc("/api/snapshot", snapshotHandler)
	mux.HandleFunc("/events", eventsHandler)
	mux.Handle("/", http.FileServer(http.Dir("public")))

	log.Printf("jsys command center: http://%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func snapshotHandler(w http.ResponseWriter, r *http.Request) {
	sample, err := sampleSystem()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, sample)
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
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

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	if sample, err := sampleSystem(); err == nil {
		sendSSE(w, "metrics", sample)
		flusher.Flush()
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			sample, err := sampleSystem()
			if err != nil {
				sendSSE(w, "error", map[string]string{"message": err.Error()})
			} else {
				sendSSE(w, "metrics", sample)
			}
			flusher.Flush()
		}
	}
}

func sampleSystem() (Snapshot, error) {
	cpu := cpuUsage()
	memory := memoryUsage()
	disks := diskUsage()
	network := networkUsage()
	processes := topProcesses()
	ports := openPorts()
	git := gitStatus()
	primaryDisk := Disk{}
	if len(disks) > 0 {
		primaryDisk = disks[0]
		for _, disk := range disks {
			if disk.Mount == "/" {
				primaryDisk = disk
				break
			}
		}
	}
	health := calculatePressure(cpu, memory, primaryDisk, network)

	sample := Snapshot{
		Timestamp: time.Now().UnixMilli(),
		Host: Host{
			Name:     hostname(),
			Platform: platformName(),
			Arch:     runtime.GOARCH,
			Uptime:   uptimeSeconds(),
			Node:     runtime.Version(),
		},
		Health:    health,
		CPU:       cpu,
		Memory:    memory,
		Disks:     disks,
		Network:   network,
		Processes: processes,
		Ports:     ports,
		Git:       git,
	}
	sample.Events = updateEvents(sample)
	return sample, nil
}

func cpuUsage() CPU {
	cores := runtime.NumCPU()
	total := 0.0
	perCore := []float64{}

	if runtime.GOOS == "linux" {
		perCore = linuxCPUUsage()
		if len(perCore) > 0 {
			for _, value := range perCore {
				total += value
			}
			total /= float64(len(perCore))
			cores = len(perCore)
		}
	} else {
		total = darwinCPUUsage()
		perCore = visualPerCore(total, cores)
	}

	return CPU{
		Total:       clamp(total, 0, 100),
		PerCore:     perCore,
		Cores:       cores,
		Model:       cpuModel(),
		LoadAverage: loadAverage(),
	}
}

func linuxCPUUsage() []float64 {
	current := readLinuxCPUTimes()
	if len(current) == 0 {
		return nil
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	before := state.previousCPU
	state.previousCPU = current
	if len(before) != len(current) {
		return visualPerCore(loadFallback(), len(current))
	}

	values := make([]float64, 0, len(current))
	for i, now := range current {
		old := before[i]
		idle := float64(now.idle - old.idle)
		total := float64(sumCPU(now) - sumCPU(old))
		value := 0.0
		if total > 0 {
			value = ((total - idle) / total) * 100
		}
		values = append(values, clamp(value, 0, 100))
	}
	return values
}

func readLinuxCPUTimes() []cpuTimes {
	if runtime.GOOS != "linux" {
		return nil
	}

	file, err := os.Open("/proc/stat")
	if err != nil {
		return nil
	}
	defer file.Close()

	var times []cpuTimes
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !regexp.MustCompile(`^cpu[0-9]+ `).MatchString(line) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		times = append(times, cpuTimes{
			user: parseUint(fields[1]),
			nice: parseUint(fields[2]),
			sys:  parseUint(fields[3]),
			idle: parseUint(fields[4]),
			wait: parseUint(fields[5]),
			irq:  parseUint(fields[6]),
			soft: parseUint(fields[7]),
		})
	}
	return times
}

func darwinCPUUsage() float64 {
	output := run(1200*time.Millisecond, "ps", "-A", "-o", "%cpu=")
	if output == "" {
		return loadFallback()
	}

	total := 0.0
	for _, line := range strings.Split(output, "\n") {
		total += parseFloat(line)
	}
	return clamp(total/float64(runtime.NumCPU()), 0, 100)
}

func visualPerCore(total float64, cores int) []float64 {
	if cores < 1 {
		cores = 1
	}
	values := make([]float64, cores)
	for i := 0; i < cores; i++ {
		wave := math.Sin(float64(time.Now().UnixMilli())/900.0+float64(i)*0.83) * 13
		slope := (float64(i%4) - 1.5) * 3.5
		values[i] = clamp(total+wave+slope, 0, 100)
	}
	return values
}

func memoryUsage() Memory {
	if runtime.GOOS == "linux" {
		return linuxMemoryUsage()
	}
	return darwinMemoryUsage()
}

func darwinMemoryUsage() Memory {
	total := parseUint(run(800*time.Millisecond, "sysctl", "-n", "hw.memsize"))
	pageSize := parseUint(run(800*time.Millisecond, "pagesize"))
	if pageSize == 0 {
		pageSize = 4096
	}

	stats := map[string]uint64{}
	for _, line := range strings.Split(run(1200*time.Millisecond, "vm_stat"), "\n") {
		line = strings.TrimSpace(strings.ReplaceAll(line, ".", ""))
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}
		stats[strings.TrimSpace(parts[0])] = parseUint(strings.TrimSpace(parts[1]))
	}

	freePages := stats["Pages free"] + stats["Pages inactive"] + stats["Pages speculative"]
	free := freePages * pageSize
	if free > total {
		free = 0
	}
	used := total - free
	swap := swapUsage()
	return Memory{Total: total, Used: used, Free: free, Percent: percent(used, total), Swap: swap}
}

func linuxMemoryUsage() Memory {
	values := parseMemInfo()
	total := values["MemTotal"] * 1024
	available := values["MemAvailable"] * 1024
	if available == 0 {
		available = values["MemFree"] * 1024
	}
	used := uint64(0)
	if total > available {
		used = total - available
	}
	swap := Swap{
		Total: values["SwapTotal"] * 1024,
		Used:  (values["SwapTotal"] - values["SwapFree"]) * 1024,
	}
	swap.Percent = percent(swap.Used, swap.Total)
	return Memory{Total: total, Used: used, Free: available, Percent: percent(used, total), Swap: swap}
}

func swapUsage() Swap {
	if runtime.GOOS == "darwin" {
		output := run(800*time.Millisecond, "sysctl", "-n", "vm.swapusage")
		match := regexp.MustCompile(`total = ([\d.]+)M.*used = ([\d.]+)M`).FindStringSubmatch(output)
		if len(match) == 3 {
			total := uint64(parseFloat(match[1]) * 1024 * 1024)
			used := uint64(parseFloat(match[2]) * 1024 * 1024)
			return Swap{Total: total, Used: used, Percent: percent(used, total)}
		}
	}

	values := parseMemInfo()
	total := values["SwapTotal"] * 1024
	used := (values["SwapTotal"] - values["SwapFree"]) * 1024
	return Swap{Total: total, Used: used, Percent: percent(used, total)}
}

func parseMemInfo() map[string]uint64 {
	result := map[string]uint64{}
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return result
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			result[strings.TrimSuffix(fields[0], ":")] = parseUint(fields[1])
		}
	}
	return result
}

func diskUsage() []Disk {
	output := run(1200*time.Millisecond, "df", "-kP")
	var disks []Disk
	for i, line := range strings.Split(output, "\n") {
		if i == 0 || strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 6 || strings.HasPrefix(parts[0], "devfs") {
			continue
		}
		disks = append(disks, Disk{
			Filesystem: parts[0],
			Size:       parseUint(parts[1]) * 1024,
			Used:       parseUint(parts[2]) * 1024,
			Available:  parseUint(parts[3]) * 1024,
			Percent:    parseFloat(strings.TrimSuffix(parts[4], "%")),
			Mount:      strings.Join(parts[5:], " "),
		})
		if len(disks) == 7 {
			break
		}
	}
	return disks
}

func networkUsage() Network {
	now := time.Now()
	current := networkCounters()

	state.mu.Lock()
	before := state.previousNet
	interval := now.Sub(state.previousAt).Seconds()
	if interval < 0.25 {
		interval = 0.25
	}
	state.previousNet = current
	state.previousAt = now
	state.mu.Unlock()

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

func networkCounters() map[string]netCounters {
	if runtime.GOOS == "linux" {
		return linuxNetworkCounters()
	}
	return bsdNetworkCounters()
}

func linuxNetworkCounters() map[string]netCounters {
	result := map[string]netCounters{}
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return result
	}
	for _, line := range strings.Split(string(data), "\n")[2:] {
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 16 {
			continue
		}
		result[strings.TrimSpace(parts[0])] = netCounters{rxBytes: parseUint(fields[0]), txBytes: parseUint(fields[8])}
	}
	return result
}

func bsdNetworkCounters() map[string]netCounters {
	result := map[string]netCounters{}
	output := run(1200*time.Millisecond, "netstat", "-ibn")
	for i, line := range strings.Split(output, "\n") {
		if i == 0 || strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		name := fields[0]
		rx := parseUint(fields[6])
		tx := parseUint(fields[9])
		current := result[name]
		if rx > current.rxBytes {
			current.rxBytes = rx
		}
		if tx > current.txBytes {
			current.txBytes = tx
		}
		result[name] = current
	}
	return result
}

func topProcesses() []ProcessInfo {
	output := run(1200*time.Millisecond, "ps", "-axo", "pid=,comm=,%cpu=,%mem=", "-r")
	var processes []ProcessInfo
	for _, line := range strings.Split(output, "\n") {
		match := processPattern.FindStringSubmatch(line)
		if len(match) != 5 {
			continue
		}
		command := filepath.Base(match[2])
		if command == "." || command == "/" || command == "" {
			command = "pid " + match[1]
		}
		processes = append(processes, ProcessInfo{
			PID:     int(parseUint(match[1])),
			Command: command,
			CPU:     parseFloat(match[3]),
			Memory:  parseFloat(match[4]),
		})
		if len(processes) == 8 {
			break
		}
	}
	return processes
}

func openPorts() []int {
	output := ""
	if runtime.GOOS == "darwin" {
		output = run(1200*time.Millisecond, "lsof", "-nP", "-iTCP", "-sTCP:LISTEN")
	} else {
		output = run(1200*time.Millisecond, "ss", "-ltnp")
	}

	seen := map[int]bool{}
	for _, line := range strings.Split(output, "\n")[1:] {
		matches := portPattern.FindAllStringSubmatch(line, -1)
		if len(matches) == 0 {
			continue
		}
		port := int(parseUint(matches[len(matches)-1][1]))
		if port > 0 {
			seen[port] = true
		}
	}

	ports := make([]int, 0, len(seen))
	for port := range seen {
		ports = append(ports, port)
	}
	sort.Ints(ports)
	if len(ports) > 16 {
		ports = ports[:16]
	}
	return ports
}

func gitStatus() GitStatus {
	branch := strings.TrimSpace(run(800*time.Millisecond, "git", "branch", "--show-current"))
	short := strings.TrimSpace(run(800*time.Millisecond, "git", "status", "--short"))
	changed := 0
	if short != "" {
		changed = len(strings.Split(short, "\n"))
	}
	if branch == "" {
		branch = "unknown"
	}
	return GitStatus{Branch: branch, ChangedFiles: changed, Clean: short == ""}
}

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

func updateEvents(sample Snapshot) []Event {
	var candidates []eventCandidate
	if sample.CPU.Total > 85 {
		candidates = append(candidates, eventCandidate{"critical", "CPU saturation", fmt.Sprintf("%.0f%% utilization across %d cores", sample.CPU.Total, sample.CPU.Cores)})
	} else if sample.CPU.Total > 65 {
		candidates = append(candidates, eventCandidate{"warn", "CPU pressure", fmt.Sprintf("%.0f%% utilization trend detected", sample.CPU.Total)})
	}

	if sample.Memory.Percent > 88 {
		candidates = append(candidates, eventCandidate{"critical", "Memory ceiling", fmt.Sprintf("%s used of %s", formatBytes(sample.Memory.Used), formatBytes(sample.Memory.Total))})
	} else if sample.Memory.Percent > 72 {
		candidates = append(candidates, eventCandidate{"warn", "Memory pressure", fmt.Sprintf("%.0f%% RAM allocation", sample.Memory.Percent)})
	}

	for _, disk := range sample.Disks {
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

	if len(sample.Processes) > 0 && sample.Processes[0].CPU > 60 {
		top := sample.Processes[0]
		candidates = append(candidates, eventCandidate{"warn", "Hot process", fmt.Sprintf("%s is using %.0f%% CPU", top.Command, top.CPU)})
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if len(candidates) == 0 && len(state.events) == 0 {
		candidates = append(candidates, eventCandidate{"info", "Telemetry online", "Live system stream established"})
	}

	now := time.Now().UnixMilli()
	for _, candidate := range candidates {
		if eventExists(state.events, candidate) {
			continue
		}
		state.eventID++
		event := Event{ID: state.eventID, Level: candidate.level, Title: candidate.title, Detail: candidate.detail, Time: now}
		state.events = append([]Event{event}, state.events...)
	}
	if len(state.events) > 12 {
		state.events = state.events[:12]
	}

	events := make([]Event, len(state.events))
	copy(events, state.events)
	return events
}

func eventExists(events []Event, candidate eventCandidate) bool {
	for _, event := range events {
		if event.Level == candidate.level && event.Title == candidate.title && event.Detail == candidate.detail {
			return true
		}
	}
	return false
}

func loadAverage() []float64 {
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/loadavg")
		if err == nil {
			fields := strings.Fields(string(data))
			if len(fields) >= 3 {
				return []float64{parseFloat(fields[0]), parseFloat(fields[1]), parseFloat(fields[2])}
			}
		}
	}

	output := run(800*time.Millisecond, "sysctl", "-n", "vm.loadavg")
	fields := regexp.MustCompile(`[\d.]+`).FindAllString(output, 3)
	if len(fields) == 3 {
		return []float64{parseFloat(fields[0]), parseFloat(fields[1]), parseFloat(fields[2])}
	}
	return []float64{0, 0, 0}
}

func loadFallback() float64 {
	load := loadAverage()
	if len(load) == 0 || runtime.NumCPU() == 0 {
		return 0
	}
	return clamp((load[0]/float64(runtime.NumCPU()))*100, 0, 100)
}

func cpuModel() string {
	if runtime.GOOS == "darwin" {
		if model := strings.TrimSpace(run(800*time.Millisecond, "sysctl", "-n", "machdep.cpu.brand_string")); model != "" {
			return model
		}
	}
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					return strings.TrimSpace(parts[1])
				}
			}
		}
	}
	return runtime.GOARCH
}

func platformName() string {
	if runtime.GOOS == "darwin" {
		release := strings.TrimSpace(run(800*time.Millisecond, "uname", "-r"))
		if release != "" {
			return "Darwin " + release
		}
	}
	if data, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "PRETTY_NAME=") {
				return strings.Trim(strings.TrimPrefix(line, "PRETTY_NAME="), `"`)
			}
		}
	}
	return runtime.GOOS
}

func uptimeSeconds() float64 {
	if runtime.GOOS == "linux" {
		data, err := os.ReadFile("/proc/uptime")
		if err == nil {
			fields := strings.Fields(string(data))
			if len(fields) > 0 {
				return parseFloat(fields[0])
			}
		}
	}
	output := strings.TrimSpace(run(800*time.Millisecond, "sysctl", "-n", "kern.boottime"))
	match := regexp.MustCompile(`sec = (\d+)`).FindStringSubmatch(output)
	if len(match) == 2 {
		boot := int64(parseUint(match[1]))
		return time.Since(time.Unix(boot, 0)).Seconds()
	}
	return 0
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "unknown-host"
	}
	return name
}

func run(timeout time.Duration, command string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, command, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func sendSSE(w http.ResponseWriter, event string, data any) {
	payload, _ := json.Marshal(data)
	fmt.Fprintf(w, "event: %s\n", event)
	fmt.Fprintf(w, "data: %s\n\n", payload)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(value)
}

func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func parseUint(value string) uint64 {
	parsed, _ := strconv.ParseUint(strings.TrimSpace(strings.TrimSuffix(value, ".")), 10, 64)
	return parsed
}

func parseFloat(value string) float64 {
	parsed, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return parsed
}

func sumCPU(value cpuTimes) uint64 {
	return value.user + value.nice + value.sys + value.idle + value.wait + value.irq + value.soft
}

func percent(used, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return (float64(used) / float64(total)) * 100
}

func clamp(value, min, max float64) float64 {
	return math.Min(math.Max(value, min), max)
}

func formatBytes(bytes uint64) string {
	if bytes == 0 {
		return "0 B"
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	index := int(math.Min(math.Floor(math.Log(float64(bytes))/math.Log(1024)), float64(len(units)-1)))
	value := float64(bytes) / math.Pow(1024, float64(index))
	if index == 0 {
		return fmt.Sprintf("%.0f %s", value, units[index])
	}
	return fmt.Sprintf("%.1f %s", value, units[index])
}
