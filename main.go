package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	host := env("HOST", "127.0.0.1")
	port := env("PORT", "4173")
	addr := host + ":" + port

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sampler := New()
	samplerDone := make(chan struct{})
	go func() {
		sampler.Run(ctx)
		close(samplerDone)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/snapshot", makeSnapshotHandler(sampler))
	mux.HandleFunc("/events", makeEventsHandler(sampler))
	mux.Handle("/", publicFileServer())

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("jsys command center: http://%s", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-samplerDone
}
