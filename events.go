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
