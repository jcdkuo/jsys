package main

import (
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
	AI        AIStatus      `json:"ai"`
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
	Total            float64   `json:"total"`
	PerCore          []float64 `json:"perCore"`
	PerCoreEstimated bool      `json:"perCoreEstimated"`
	Cores            int       `json:"cores"`
	Model            string    `json:"model"`
	LoadAverage      []float64 `json:"loadAverage"`
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

type AIStatus struct {
	Agents  []AIAgent          `json:"agents"`
	Remotes []RemoteConnection `json:"remotes"`
}

type AIAgent struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
	Scope string `json:"scope"`
}

type RemoteConnection struct {
	Target   string `json:"target"`
	Host     string `json:"host"`
	User     string `json:"user"`
	Port     string `json:"port"`
	Source   string `json:"source"`
	Sessions int    `json:"sessions"`
	PIDs     []int  `json:"pids"`
	Tunnel   bool   `json:"tunnel"`
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

type procInfo struct {
	pid     int
	ppid    int
	command string
}
