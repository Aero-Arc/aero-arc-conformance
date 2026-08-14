// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package app owns process lifecycle and management endpoints.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/config"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	telemetryinflux "github.com/aero-arc/aero-arc-conformance/internal/telemetry/influx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type App struct {
	cfg    config.Config
	log    *slog.Logger
	store  *postgresstore.Store
	reader *telemetryinflux.Reader
	server *http.Server
	ready  atomic.Bool
}

// New constructs app from the supplied configuration and dependencies.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the operation.
//   - cfg: provides the configuration values used to initialize or execute the operation.
//   - log: is the *slog.Logger value supplied to New.
//
// Returns:
//   - result: is the *App value produced by New.
//   - error: reports validation, dependency, cancellation, or persistence failures.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	store, err := postgresstore.Open(ctx, cfg.Postgres.URL)
	if err != nil {
		return nil, err
	}
	reader, err := telemetryinflux.New(cfg.Influx.Host, cfg.Influx.Token, cfg.Influx.Database, cfg.Influx.AircraftBatchSize, cfg.Influx.MaxRows)
	if err != nil {
		store.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	a := &App{cfg: cfg, log: log, store: store, reader: reader}
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", a.handleReady)
	a.server = &http.Server{Addr: cfg.Service.ManagementAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	a.ready.Store(true)
	return a, nil
}

// Run serves the management endpoint until context cancellation or a terminal
// HTTP server failure. Cancellation performs graceful shutdown and returns the
// shutdown result rather than the context cancellation error.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the operation.
//
// Returns:
//   - error: reports graceful-shutdown/close failure or a wrapped ListenAndServe failure.
func (a *App) Run(ctx context.Context) error {
	errors := make(chan error, 1)
	go func() {
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errors <- err
		}
	}()
	select {
	case <-ctx.Done():
		return a.Shutdown()
	case err := <-errors:
		_ = a.Shutdown()
		return fmt.Errorf("management server: %w", err)
	}
}
func (a *App) handleReady(w http.ResponseWriter, r *http.Request) {
	if !a.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.store.Ping(ctx); err != nil {
		http.Error(w, "postgres unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

// Shutdown stops App and releases its owned resources.
//
// Returns:
//   - error: reports validation, dependency, cancellation, or persistence failures.
func (a *App) Shutdown() error {
	a.ready.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.Service.ShutdownTimeout.Value())
	defer cancel()
	serverErr := a.server.Shutdown(ctx)
	readerErr := a.reader.Close()
	a.store.Close()
	if serverErr != nil {
		return serverErr
	}
	return readerErr
}
