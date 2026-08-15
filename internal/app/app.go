// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package app owns process lifecycle and management endpoints.
package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/config"
	registryprojection "github.com/aero-arc/aero-arc-conformance/internal/projection/registry"
	postgresstore "github.com/aero-arc/aero-arc-conformance/internal/store/postgres"
	telemetryinflux "github.com/aero-arc/aero-arc-conformance/internal/telemetry/influx"
	assignmentgrpc "github.com/aero-arc/aero-arc-conformance/internal/transport/grpc"
	registryv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/registry/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// App owns the Conformance process resources, ingress servers, and Registry
// publisher lifecycle.
type App struct {
	cfg                config.Config
	log                *slog.Logger
	store              *postgresstore.Store
	reader             *telemetryinflux.Reader
	managementServer   *http.Server
	assignmentServer   *assignmentgrpc.Server
	registryConnection *gogrpc.ClientConn
	publisher          *registryprojection.Publisher
	runCancel          context.CancelFunc
	ready              atomic.Bool
	shutdownOnce       sync.Once
	shutdownErr        error
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
	assignmentServer, err := assignmentgrpc.New(store)
	if err != nil {
		_ = reader.Close()
		store.Close()
		return nil, err
	}
	var transportCredentials credentials.TransportCredentials
	if cfg.Registry.Insecure {
		transportCredentials = insecure.NewCredentials()
	} else {
		transportCredentials = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	registryConnection, err := gogrpc.NewClient(cfg.Registry.Address, gogrpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		_ = reader.Close()
		store.Close()
		return nil, fmt.Errorf("create Registry client: %w", err)
	}
	publisher, err := registryprojection.New(store, registryv1.NewAeroRegistryClient(registryConnection), registryprojection.Config{
		WorkerID: cfg.Worker.ID + ":registry", PollInterval: cfg.Registry.PublishInterval.Value(),
		RequestTimeout: cfg.Registry.RequestTimeout.Value(), LeaseDuration: cfg.Registry.LeaseDuration.Value(),
		RetryDelay: cfg.Registry.RetryDelay.Value(), BatchSize: cfg.Registry.BatchSize,
	}, log)
	if err != nil {
		_ = registryConnection.Close()
		_ = reader.Close()
		store.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	a := &App{cfg: cfg, log: log, store: store, reader: reader, assignmentServer: assignmentServer, registryConnection: registryConnection, publisher: publisher}
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", a.handleReady)
	a.managementServer = &http.Server{Addr: cfg.Service.ManagementAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	a.ready.Store(true)
	return a, nil
}

// Run serves management and assignment gRPC endpoints while publishing the
// durable Registry outbox. A terminal server failure or context cancellation
// performs bounded graceful shutdown.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the operation.
//
// Returns:
//   - error: reports graceful-shutdown/close failure or a wrapped ListenAndServe failure.
func (a *App) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	a.runCancel = cancel
	listener, err := net.Listen("tcp", a.cfg.Service.GRPCAddress)
	if err != nil {
		_ = a.Shutdown()
		return fmt.Errorf("listen for assignment gRPC: %w", err)
	}
	errCh := make(chan error, 2)
	go func() {
		if serveErr := a.managementServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- fmt.Errorf("management server: %w", serveErr)
		}
	}()
	go func() {
		if serveErr := a.assignmentServer.Serve(listener); serveErr != nil && !errors.Is(serveErr, gogrpc.ErrServerStopped) {
			errCh <- fmt.Errorf("assignment gRPC server: %w", serveErr)
		}
	}()
	go func() { _ = a.publisher.Run(runCtx) }()
	select {
	case <-runCtx.Done():
		return a.Shutdown()
	case err := <-errCh:
		_ = a.Shutdown()
		return err
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
	a.shutdownOnce.Do(func() {
		a.ready.Store(false)
		if a.runCancel != nil {
			a.runCancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), a.cfg.Service.ShutdownTimeout.Value())
		defer cancel()
		managementErr := a.managementServer.Shutdown(ctx)
		grpcDone := make(chan struct{})
		go func() {
			a.assignmentServer.GracefulStop()
			close(grpcDone)
		}()
		select {
		case <-grpcDone:
		case <-ctx.Done():
			a.assignmentServer.Stop()
			<-grpcDone
		}
		connectionErr := a.registryConnection.Close()
		readerErr := a.reader.Close()
		a.store.Close()
		a.shutdownErr = errors.Join(managementErr, connectionErr, readerErr)
	})
	return a.shutdownErr
}
