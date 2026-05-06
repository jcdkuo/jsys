package main

import (
	"testing"
)

func TestCalculatePressureZero(t *testing.T) {
	h := calculatePressure(CPU{Cores: 8}, Memory{}, Disk{}, Network{})
	if h.Score != 0 {
		t.Errorf("score on zero input = %v, want 0", h.Score)
	}
	if h.State != "Stable" {
		t.Errorf("state = %q, want Stable", h.State)
	}
}

func TestCalculatePressureStableBelowThreshold(t *testing.T) {
	h := calculatePressure(
		CPU{Total: 50, Cores: 8, LoadAverage: []float64{1}},
		Memory{Percent: 30},
		Disk{Percent: 40},
		Network{},
	)
	if h.State != "Stable" {
		t.Errorf("score=%v state=%q, want Stable", h.Score, h.State)
	}
}

func TestCalculatePressureBoundaries(t *testing.T) {
	// 100*0.34 + 100*0.28 = 62.0 → exactly at Pressure threshold.
	h := calculatePressure(CPU{Total: 100, Cores: 8}, Memory{Percent: 100}, Disk{}, Network{})
	if h.Score != 62 {
		t.Errorf("score = %v, want 62", h.Score)
	}
	if h.State != "Pressure" {
		t.Errorf("at 62 state = %q, want Pressure", h.State)
	}

	// 100*0.34 + 100*0.28 + 100*0.20 = 82.0 → exactly at Critical threshold.
	h = calculatePressure(CPU{Total: 100, Cores: 8}, Memory{Percent: 100}, Disk{Percent: 100}, Network{})
	if h.Score != 82 {
		t.Errorf("score = %v, want 82", h.Score)
	}
	if h.State != "Critical" {
		t.Errorf("at 82 state = %q, want Critical", h.State)
	}
}

func TestCalculatePressureHandlesEmptyLoadAverage(t *testing.T) {
	h := calculatePressure(CPU{Total: 50, Cores: 8}, Memory{}, Disk{}, Network{})
	expected := 50 * 0.34
	if h.Score != expected {
		t.Errorf("missing LoadAverage shouldn't contribute; score=%v want %v", h.Score, expected)
	}
}

func TestCalculatePressureHandlesZeroCores(t *testing.T) {
	h := calculatePressure(CPU{Total: 50, Cores: 0, LoadAverage: []float64{99}}, Memory{}, Disk{}, Network{})
	expected := 50 * 0.34 // load contribution must be 0 when cores=0
	if h.Score != expected {
		t.Errorf("zero cores shouldn't contribute load; score=%v want %v", h.Score, expected)
	}
}

func TestCalculatePressureScoreClamped(t *testing.T) {
	// Push every input to extreme; result must clamp at 100.
	h := calculatePressure(
		CPU{Total: 200, Cores: 1, LoadAverage: []float64{200}},
		Memory{Percent: 200},
		Disk{Percent: 200},
		Network{RxRate: 1e15, TxRate: 1e15},
	)
	if h.Score != 100 {
		t.Errorf("score = %v, want clamped 100", h.Score)
	}
	if h.State != "Critical" {
		t.Errorf("state = %q, want Critical", h.State)
	}
}

func TestUpdateEventsLockedTelemetryOnFirstQuietCall(t *testing.T) {
	s := New()
	snap := &Snapshot{}
	out := s.updateEventsLocked(snap)
	if len(out) != 1 || out[0].Title != "Telemetry online" {
		t.Fatalf("expected single Telemetry online event, got %+v", out)
	}
}

func TestUpdateEventsLockedTriggersCPUSaturation(t *testing.T) {
	s := New()
	snap := &Snapshot{CPU: CPU{Total: 90, Cores: 8}}
	out := s.updateEventsLocked(snap)
	if len(out) != 1 || out[0].Level != "critical" || out[0].Title != "CPU saturation" {
		t.Fatalf("expected CPU saturation critical, got %+v", out)
	}
}

func TestUpdateEventsLockedDedupesIdenticalEvent(t *testing.T) {
	s := New()
	snap := &Snapshot{CPU: CPU{Total: 90, Cores: 8}}
	first := s.updateEventsLocked(snap)
	second := s.updateEventsLocked(snap)
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected 1 event each call, got first=%d second=%d", len(first), len(second))
	}
	if first[0].ID != second[0].ID {
		t.Errorf("dedup should reuse event; first ID=%d second ID=%d", first[0].ID, second[0].ID)
	}
}

func TestUpdateEventsLockedDistinctDetailsCreateNewEvent(t *testing.T) {
	s := New()
	s.updateEventsLocked(&Snapshot{CPU: CPU{Total: 90, Cores: 8}})
	out := s.updateEventsLocked(&Snapshot{CPU: CPU{Total: 91, Cores: 8}})
	if len(out) != 2 {
		t.Fatalf("expected 2 events after distinct details, got %d", len(out))
	}
	// Newest first.
	if out[0].ID <= out[1].ID {
		t.Errorf("expected newest event first; got IDs %d then %d", out[0].ID, out[1].ID)
	}
}

func TestUpdateEventsLockedRingTrimsAtTwelve(t *testing.T) {
	s := New()
	for i := 0; i < 20; i++ {
		s.updateEventsLocked(&Snapshot{
			CPU:       CPU{Total: 90 + float64(i)/100, Cores: 8},
			Processes: []ProcessInfo{{Command: "x", CPU: 70}}, // distinct hot-process detail
			Memory:    Memory{Percent: 89, Used: uint64(i), Total: 100},
		})
	}
	out := s.updateEventsLocked(&Snapshot{})
	if len(out) > 12 {
		t.Errorf("ring not trimmed; len=%d", len(out))
	}
}
