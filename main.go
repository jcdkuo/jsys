package main

import (
	"log"
	"net/http"
)

func main() {
	host := env("HOST", "127.0.0.1")
	port := env("PORT", "4173")
	addr := host + ":" + port

	mux := http.NewServeMux()
	// mux.HandleFunc("/api/snapshot", snapshotHandler)
	// mux.HandleFunc("/events", eventsHandler)
	mux.Handle("/", http.FileServer(http.Dir("public")))

	log.Printf("jsys command center: http://%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
