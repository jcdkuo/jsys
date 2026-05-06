package main

import (
	"bufio"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

var (
	processPattern = regexp.MustCompile(`^\s*(\d+)\s+(.+?)\s+([\d.]+)\s+([\d.]+)\s*$`)
	portPattern    = regexp.MustCompile(`[:.]([0-9]+)([[:space:]]|\)|$)`)
)

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
		if version := strings.TrimSpace(run(800*time.Millisecond, "sw_vers", "-productVersion")); version != "" {
			return "macOS " + version
		}
		if release := strings.TrimSpace(run(800*time.Millisecond, "uname", "-r")); release != "" {
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
