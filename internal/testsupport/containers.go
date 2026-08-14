//go:build integration

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package testsupport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	PostgresImage  = "postgis/postgis:14-3.5-alpine"
	InfluxImage    = "influxdb:3.10.3-core"
	InfluxDatabase = "aero_arc_conformance_test"
	InfluxToken    = "integration-test-token"
)

type Dependency struct {
	container testcontainers.Container
	name      string
}

// Shutdown stops Dependency and releases its owned resources.
//
// Parameters:
//   - failed: indicates whether diagnostics for a failed test should be emitted.
//   - output: receives encoded output or diagnostic details.
//
// Returns:
//   - error: reports validation, dependency, cancellation, or persistence failures.
func (d *Dependency) Shutdown(failed bool, output io.Writer) error {
	if d == nil || d.container == nil {
		return nil
	}
	if failed {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		logs, err := d.container.Logs(ctx)
		if err == nil {
			_, _ = fmt.Fprintf(output, "--- %s logs ---\n", d.name)
			_, _ = io.Copy(output, logs)
			_ = logs.Close()
		}
		cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return testcontainers.TerminateContainer(d.container, testcontainers.StopContext(ctx))
}

type Postgres struct {
	URL        string
	Dependency *Dependency
}

// StartPostgres starts a pinned PostGIS Testcontainer, waits for SQL readiness,
// and returns dynamically mapped connection details with bounded cleanup.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the operation.
//
// Returns:
//   - result: is the *Postgres value produced by StartPostgres.
//   - error: reports validation, dependency, cancellation, or persistence failures.
func StartPostgres(ctx context.Context) (*Postgres, error) {
	container, err := testcontainers.Run(ctx, PostgresImage, testcontainers.WithExposedPorts("5432/tcp"), testcontainers.WithEnv(map[string]string{"POSTGRES_DB": "conformance", "POSTGRES_USER": "conformance", "POSTGRES_PASSWORD": "conformance"}), testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		return nil, err
	}
	d := &Dependency{container: container, name: "Postgres"}
	host, err := container.Host(ctx)
	if err != nil {
		_ = d.Shutdown(true, io.Discard)
		return nil, err
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = d.Shutdown(true, io.Discard)
		return nil, err
	}
	u := (&url.URL{Scheme: "postgres", User: url.UserPassword("conformance", "conformance"), Host: net.JoinHostPort(host, port.Port()), Path: "conformance", RawQuery: "sslmode=disable"}).String()
	pool, err := pgxpool.New(ctx, u)
	if err != nil {
		_ = d.Shutdown(true, io.Discard)
		return nil, err
	}
	defer pool.Close()
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = pool.Ping(pingCtx)
		cancel()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			_ = d.Shutdown(true, io.Discard)
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return &Postgres{URL: u, Dependency: d}, nil
}

type Influx struct {
	Host, Token, Database string
	Dependency            *Dependency
}

// StartInflux starts a pinned InfluxDB Core Testcontainer, provisions the test
// database, and returns dynamically mapped client settings with bounded cleanup.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the operation.
//
// Returns:
//   - result: is the *Influx value produced by StartInflux.
//   - error: reports validation, dependency, cancellation, or persistence failures.
func StartInflux(ctx context.Context) (*Influx, error) {
	container, err := testcontainers.Run(ctx, InfluxImage, testcontainers.WithExposedPorts("8181/tcp"), testcontainers.WithCmd("influxdb3", "serve", "--node-id=conformance-integration", "--object-store=memory", "--without-auth"), testcontainers.WithWaitStrategy(wait.ForHTTP("/health").WithPort("8181/tcp").WithStartupTimeout(90*time.Second)))
	if err != nil {
		return nil, err
	}
	d := &Dependency{container: container, name: "InfluxDB"}
	host, err := container.Host(ctx)
	if err != nil {
		_ = d.Shutdown(true, io.Discard)
		return nil, err
	}
	port, err := container.MappedPort(ctx, "8181/tcp")
	if err != nil {
		_ = d.Shutdown(true, io.Discard)
		return nil, err
	}
	endpoint := (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, port.Port())}).String()
	body, _ := json.Marshal(map[string]string{"db": InfluxDatabase})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/v3/configure/database", bytes.NewReader(body))
	if err != nil {
		_ = d.Shutdown(true, io.Discard)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		_ = d.Shutdown(true, io.Discard)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = d.Shutdown(true, io.Discard)
		return nil, fmt.Errorf("create influx database: %s", resp.Status)
	}
	return &Influx{Host: endpoint, Token: InfluxToken, Database: InfluxDatabase, Dependency: d}, nil
}
