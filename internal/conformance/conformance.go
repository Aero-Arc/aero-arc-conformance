// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package conformance owns the Conformance service lifecycle and management
// endpoints.
package conformance

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
	conformancegrpc "github.com/aero-arc/aero-arc-conformance/internal/transport/grpc"
	conformancev1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/conformance/v1"
	registryv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/registry/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

// Conformance owns the service's process resources, ingress servers, and
// Registry publisher lifecycle.
type Conformance struct {
	cfg                config.Config
	log                *slog.Logger
	store              *postgresstore.Store
	reader             *telemetryinflux.Reader
	managementServer   *http.Server
	grpcServer         *gogrpc.Server
	registryConnection *gogrpc.ClientConn
	publisher          *registryprojection.Publisher
	runCancel          context.CancelFunc
	ready              atomic.Bool
	shutdownOnce       sync.Once
	shutdownErr        error
}

// New constructs the Conformance service from the supplied configuration and
// dependencies.
//
// Parameters:
//   - ctx: bounds initial PostgreSQL connection and migration work; cancelling
//     it after New returns does not stop the service.
//   - cfg: defines the durable store, bounded telemetry reader, ingress,
//     Registry publisher, and graceful-shutdown settings.
//   - log: receives service and Registry publisher diagnostics.
//
// Returns:
//   - conformance: owns all opened process resources and is ready to run; no
//     assignment or evaluation authority is changed during construction.
//   - error: reports PostgreSQL, telemetry reader, assignment server, Registry
//     connection, or publisher initialization failure. Resources opened before
//     a failure are closed before the error is returned.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*Conformance, error) {
	store, err := postgresstore.Open(ctx, cfg.Postgres.URL)
	if err != nil {
		return nil, err
	}
	reader, err := telemetryinflux.New(cfg.Influx.Host, cfg.Influx.Token, cfg.Influx.Database, cfg.Influx.AircraftBatchSize, cfg.Influx.MaxRows)
	if err != nil {
		store.Close()
		return nil, err
	}
	assignmentHandler, err := conformancegrpc.NewAssignmentHandler(store)
	if err != nil {
		_ = reader.Close()
		store.Close()
		return nil, err
	}
	grpcServer := gogrpc.NewServer()
	conformancev1.RegisterConformanceServiceServer(grpcServer, assignmentHandler)
	reflection.Register(grpcServer)
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
	conformance := &Conformance{cfg: cfg, log: log, store: store, reader: reader, grpcServer: grpcServer, registryConnection: registryConnection, publisher: publisher}
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", conformance.handleReady)
	conformance.managementServer = &http.Server{Addr: cfg.Service.ManagementAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	conformance.ready.Store(true)
	return conformance, nil
}

// Run serves management and assignment gRPC endpoints while publishing the
// durable Registry outbox. A terminal server failure or context cancellation
// performs bounded graceful shutdown.
//
// Parameters:
//   - ctx: defines the service lifetime. Cancellation stops publication and
//     initiates bounded shutdown; it does not alter assignment generations or
//     evaluation authority.
//
// Returns:
//   - error: reports assignment listener, management server, assignment gRPC
//     server, or graceful-shutdown failure. Context cancellation itself is not
//     returned when owned resources close successfully.
func (c *Conformance) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	c.runCancel = cancel
	listener, err := net.Listen("tcp", c.cfg.Service.GRPCAddress)
	if err != nil {
		_ = c.Shutdown()
		return fmt.Errorf("listen for assignment gRPC: %w", err)
	}
	errCh := make(chan error, 2)
	go func() {
		if serveErr := c.managementServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- fmt.Errorf("management server: %w", serveErr)
		}
	}()
	go func() {
		if serveErr := c.grpcServer.Serve(listener); serveErr != nil && !errors.Is(serveErr, gogrpc.ErrServerStopped) {
			errCh <- fmt.Errorf("assignment gRPC server: %w", serveErr)
		}
	}()
	go func() { _ = c.publisher.Run(runCtx) }()
	select {
	case <-runCtx.Done():
		return c.Shutdown()
	case err := <-errCh:
		_ = c.Shutdown()
		return err
	}
}

func (c *Conformance) handleReady(w http.ResponseWriter, r *http.Request) {
	if !c.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := c.store.Ping(ctx); err != nil {
		http.Error(w, "postgres unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

// Shutdown stops the Conformance service and releases its owned resources.
// Repeated calls are idempotent and return the result of the first shutdown.
// Shutdown does not delete assignments, checkpoints, evidence, or durable
// outbox rows.
//
// Returns:
//   - error: joins management server, Registry connection, and telemetry reader
//     close failures; PostgreSQL close has no error result.
func (c *Conformance) Shutdown() error {
	c.shutdownOnce.Do(func() {
		c.ready.Store(false)
		if c.runCancel != nil {
			c.runCancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.Service.ShutdownTimeout.Value())
		defer cancel()
		managementErr := c.managementServer.Shutdown(ctx)
		grpcDone := make(chan struct{})
		go func() {
			c.grpcServer.GracefulStop()
			close(grpcDone)
		}()
		select {
		case <-grpcDone:
		case <-ctx.Done():
			c.grpcServer.Stop()
			<-grpcDone
		}
		connectionErr := c.registryConnection.Close()
		readerErr := c.reader.Close()
		c.store.Close()
		c.shutdownErr = errors.Join(managementErr, connectionErr, readerErr)
	})
	return c.shutdownErr
}
