package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "jsys-tests")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmp)
	if err := os.Setenv("JSYS_STATE_PATH", filepath.Join(tmp, "state.json")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestPersistRoundTrip(t *testing.T) {
	t.Setenv("JSYS_STATE_PATH", filepath.Join(t.TempDir(), "state.json"))
	want := []Event{
		{ID: 1, Level: "warn", Title: "a", Detail: "x", Time: 1000},
		{ID: 2, Level: "critical", Title: "b", Detail: "y", Time: 2000},
	}
	if err := saveState(want, 42); err != nil {
		t.Fatalf("saveState: %v", err)
	}
	got, eventID := loadState()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events round-trip: got %+v want %+v", got, want)
	}
	if eventID != 42 {
		t.Errorf("eventID round-trip: got %d want 42", eventID)
	}
}

func TestLoadStateMissingFileIsZeroValue(t *testing.T) {
	t.Setenv("JSYS_STATE_PATH", filepath.Join(t.TempDir(), "no-such-file.json"))
	events, eventID := loadState()
	if events != nil || eventID != 0 {
		t.Fatalf("missing file should be zero, got events=%+v id=%d", events, eventID)
	}
}

func TestLoadStateMalformedJSONIsZeroValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("not json {{{"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JSYS_STATE_PATH", path)
	events, eventID := loadState()
	if events != nil || eventID != 0 {
		t.Fatalf("malformed file should be zero, got events=%+v id=%d", events, eventID)
	}
}

func TestLoadStateWrongVersionIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrong-ver.json")
	if err := os.WriteFile(path, []byte(`{"version": 999, "eventId": 5}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JSYS_STATE_PATH", path)
	events, eventID := loadState()
	if events != nil || eventID != 0 {
		t.Fatalf("wrong version should be zero, got events=%+v id=%d", events, eventID)
	}
}

func TestSaveStateAtomicRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	t.Setenv("JSYS_STATE_PATH", path)
	if err := saveState(nil, 7); err != nil {
		t.Fatalf("saveState: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("final file missing: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf(".tmp file should be gone: %v", err)
	}
}

func TestNewSamplerLoadsPersistedEvents(t *testing.T) {
	t.Setenv("JSYS_STATE_PATH", filepath.Join(t.TempDir(), "state.json"))
	if err := saveState([]Event{{ID: 9, Title: "x"}}, 9); err != nil {
		t.Fatal(err)
	}
	s := New()
	if len(s.events) != 1 || s.events[0].ID != 9 || s.eventID != 9 {
		t.Fatalf("expected loaded state, got events=%+v eventID=%d", s.events, s.eventID)
	}
}
