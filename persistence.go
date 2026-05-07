package main

import (
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

type persistedState struct {
	Version int     `json:"version"`
	Events  []Event `json:"events"`
	EventID int64   `json:"eventId"`
}

const persistenceVersion = 1

func statePath() (string, error) {
	if override := os.Getenv("JSYS_STATE_PATH"); override != "" {
		return override, nil
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cacheDir, "jsys", "state.json"), nil
}

func loadState() (events []Event, eventID int64) {
	path, err := statePath()
	if err != nil {
		return nil, 0
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Printf("loadState %s: %v", path, err)
		}
		return nil, 0
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		log.Printf("loadState parse %s: %v", path, err)
		return nil, 0
	}
	if ps.Version != persistenceVersion {
		return nil, 0
	}
	return ps.Events, ps.EventID
}

func saveState(events []Event, eventID int64) error {
	path, err := statePath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(persistedState{
		Version: persistenceVersion,
		Events:  events,
		EventID: eventID,
	})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
