// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package config loads and validates Conformance process configuration.
package config

import (
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service  Service  `yaml:"service"`
	Postgres Postgres `yaml:"postgres"`
	Influx   Influx   `yaml:"influx"`
	Worker   Worker   `yaml:"worker"`
	Policy   Policy   `yaml:"policy"`
	Logging  Logging  `yaml:"logging"`
}
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", node.Value, err)
	}
	*d = Duration(value)
	return nil
}
func (d Duration) Value() time.Duration { return time.Duration(d) }

type Service struct {
	ManagementAddress string   `yaml:"management_address"`
	ShutdownTimeout   Duration `yaml:"shutdown_timeout"`
}
type Postgres struct {
	URL string `yaml:"url"`
}
type Influx struct {
	Host              string   `yaml:"host"`
	Token             string   `yaml:"token"`
	Database          string   `yaml:"database"`
	PollInterval      Duration `yaml:"poll_interval"`
	OverlapWindow     Duration `yaml:"overlap_window"`
	SettleDelay       Duration `yaml:"settle_delay"`
	AircraftBatchSize int      `yaml:"aircraft_batch_size"`
	MaxRows           int      `yaml:"max_rows"`
}
type Worker struct {
	ID             string   `yaml:"id"`
	LeaseDuration  Duration `yaml:"lease_duration"`
	RenewInterval  Duration `yaml:"renew_interval"`
	ClaimBatchSize int      `yaml:"claim_batch_size"`
}
type Policy struct {
	Version              string   `yaml:"version"`
	HorizontalToleranceM float64  `yaml:"horizontal_tolerance_m"`
	VerticalToleranceM   float64  `yaml:"vertical_tolerance_m"`
	OpenAfterSamples     int      `yaml:"open_after_samples"`
	RecoverAfterSamples  int      `yaml:"recover_after_samples"`
	TelemetryFreshness   Duration `yaml:"telemetry_freshness"`
}
type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

func Default() Config {
	return Config{Service: Service{ManagementAddress: ":2112", ShutdownTimeout: Duration(15 * time.Second)}, Influx: Influx{PollInterval: Duration(time.Second), OverlapWindow: Duration(30 * time.Second), SettleDelay: Duration(2 * time.Second), AircraftBatchSize: 100, MaxRows: 10000}, Worker: Worker{LeaseDuration: Duration(30 * time.Second), RenewInterval: Duration(10 * time.Second), ClaimBatchSize: 20}, Policy: Policy{Version: "standard-v1", HorizontalToleranceM: 5, VerticalToleranceM: 3, OpenAfterSamples: 3, RecoverAfterSamples: 3, TelemetryFreshness: Duration(15 * time.Second)}, Logging: Logging{Level: "info", Format: "json"}}
}
func Load(path string) (Config, error) {
	cfg := Default()
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	decoder := yaml.NewDecoder(strings.NewReader(os.ExpandEnv(string(contents))))
	decoder.KnownFields(true)
	if err = decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("configuration must contain exactly one YAML document")
		}
		return Config{}, fmt.Errorf("decode trailing configuration: %w", err)
	}
	if err = cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
func (c Config) Validate() error {
	if strings.TrimSpace(c.Service.ManagementAddress) == "" || c.Service.ShutdownTimeout <= 0 {
		return fmt.Errorf("service management address and positive shutdown timeout are required")
	}
	if strings.TrimSpace(c.Postgres.URL) == "" {
		return fmt.Errorf("postgres.url is required")
	}
	if strings.TrimSpace(c.Influx.Host) == "" || strings.TrimSpace(c.Influx.Database) == "" || c.Influx.PollInterval <= 0 || c.Influx.OverlapWindow <= 0 || c.Influx.SettleDelay < 0 || c.Influx.AircraftBatchSize < 1 || c.Influx.MaxRows < 1 {
		return fmt.Errorf("influx configuration is incomplete")
	}
	if strings.TrimSpace(c.Worker.ID) == "" || c.Worker.LeaseDuration <= 0 || c.Worker.RenewInterval <= 0 || c.Worker.RenewInterval >= c.Worker.LeaseDuration || c.Worker.ClaimBatchSize < 1 {
		return fmt.Errorf("worker configuration is invalid")
	}
	if strings.TrimSpace(c.Policy.Version) == "" || !finiteNonnegative(c.Policy.HorizontalToleranceM) || !finiteNonnegative(c.Policy.VerticalToleranceM) || c.Policy.OpenAfterSamples < 1 || c.Policy.RecoverAfterSamples < 1 || c.Policy.TelemetryFreshness <= 0 {
		return fmt.Errorf("policy configuration is invalid")
	}
	if c.Logging.Format != "json" && c.Logging.Format != "text" {
		return fmt.Errorf("logging.format must be json or text")
	}
	if c.Logging.Level != "debug" && c.Logging.Level != "info" && c.Logging.Level != "warn" && c.Logging.Level != "error" {
		return fmt.Errorf("logging.level must be debug, info, warn, or error")
	}
	return nil
}

func finiteNonnegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}
