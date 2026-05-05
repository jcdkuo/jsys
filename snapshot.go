package main

import (
	"runtime"
	"time"
)

func sampleSystem() (Snapshot, error) {
	cpu := cpuUsage()
	memory := memoryUsage()
	disks := diskUsage()
	network := networkUsage()
	processes := topProcesses()
	ports := openPorts()
	git := gitStatus()
	ai := aiStatus()
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
		AI:        ai,
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
