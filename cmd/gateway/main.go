// Command gateway runs the inference-guard API gateway: a multi-tenant,
// OpenAI-compatible entry point in front of a model inference server.
package main

import (
	"log/slog"
	"os"

	"github.com/nynrathod/inference-guard/internal/auth"
	"github.com/nynrathod/inference-guard/internal/config"
	"github.com/nynrathod/inference-guard/internal/proxy"
	"github.com/nynrathod/inference-guard/internal/router"
	"github.com/nynrathod/inference-guard/internal/server"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	srv := server.New(
		server.Config{
			Addr:              cfg.Addr,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			ShutdownTimeout:   cfg.ShutdownTimeout,
		},
		router.New(
			log,
			auth.New(cfg.APIKeys, log),
			proxy.New(cfg.UpstreamURL, log).Handler(),
		),
		log,
	)

	if err := srv.Run(); err != nil {
		log.Error("gateway terminated", "error", err)
		os.Exit(1)
	}
}
