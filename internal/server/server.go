package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Config carries the HTTP server settings of the gateway.
type Config struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration
}

// Server owns the HTTP lifecycle: listen, serve, and drain in-flight
// requests when the process is asked to stop.
type Server struct {
	http  *http.Server
	log   *slog.Logger
	drain time.Duration
}

// New returns a Server bound to cfg.Addr serving handler.
func New(cfg Config, handler http.Handler, log *slog.Logger) *Server {
	return &Server{
		http: &http.Server{
			Addr:    cfg.Addr,
			Handler: handler,
			// No WriteTimeout: streamed completions are long-lived by design.
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		},
		log:   log,
		drain: cfg.ShutdownTimeout,
	}
}

// Run blocks until SIGINT or SIGTERM arrives, then drains connections.
func (s *Server) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("gateway listening", "addr", s.http.Addr)
		if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	s.log.Info("draining connections")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.drain)
	defer cancel()
	if err := s.http.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	s.log.Info("gateway stopped")
	return nil
}
