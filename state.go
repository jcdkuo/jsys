package main

import (
	"regexp"
	"time"
)

var (
	state = &appState{
		previousCPU: readLinuxCPUTimes(),
		previousNet: map[string]netCounters{},
		previousAt:  time.Now(),
	}
	processPattern = regexp.MustCompile(`^\s*(\d+)\s+(.+?)\s+([\d.]+)\s+([\d.]+)\s*$`)
	portPattern    = regexp.MustCompile(`[:.]([0-9]+)([[:space:]]|\)|$)`)
)
