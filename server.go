package main

import (
	"net/http"
	"time"
)

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
