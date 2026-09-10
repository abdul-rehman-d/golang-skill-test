package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang-skill-test/internal/jobs"
)

func main() {
	service := jobs.NewService(2, 4)
	mux := http.NewServeMux()
	jobs.RegisterHandlers(mux, service)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Printf("server listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Mark the service as stopping first so active or newly arrived POST requests
	// cannot enqueue work while the HTTP server drains its handlers.
	service.Stop()

	ctx, cancel := signalContext()
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("graceful HTTP shutdown failed: %v", err)
		if closeErr := server.Close(); closeErr != nil {
			log.Printf("force HTTP shutdown failed: %v", closeErr)
		}
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
