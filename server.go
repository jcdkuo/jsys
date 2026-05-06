package main

import (
	"net/http"
)

func makeSnapshotHandler(s *Sampler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := s.Latest()
		if snap == nil {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "sampler warming up", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, snap)
	}
}

func makeEventsHandler(s *Sampler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		if snap := s.Latest(); snap != nil {
			sendSSE(w, "metrics", snap)
			flusher.Flush()
		}

		ch, unsub := s.Subscribe()
		defer unsub()

		for {
			select {
			case <-r.Context().Done():
				return
			case snap, ok := <-ch:
				if !ok {
					return
				}
				sendSSE(w, "metrics", snap)
				flusher.Flush()
			}
		}
	}
}
