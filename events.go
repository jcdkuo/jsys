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
