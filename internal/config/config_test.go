// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package config

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("service:\n  typo: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("unknown field was accepted")
	}
}

func TestValidateRejectsUnsafePolicyAndLogging(t *testing.T) {
	cfg := Default()
	cfg.Postgres.URL = "postgres://example"
	cfg.Influx.Host = "http://example"
	cfg.Influx.Database = "telemetry"
	cfg.Registry.Address = "registry:50051"
	cfg.Worker.ID = "worker"
	cfg.Policy.HorizontalToleranceM = math.NaN()
	if err := cfg.Validate(); err == nil {
		t.Fatal("NaN tolerance was accepted")
	}
	cfg.Policy.HorizontalToleranceM = 0
	cfg.Logging.Level = "verbose"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unknown logging level was accepted")
	}
}

func TestLoadRejectsMultipleDocuments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("service: {}\n---\nservice: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("multiple YAML documents were accepted")
	}
}
func TestDefaultRequiresDependencies(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err == nil {
		t.Fatal("empty dependency configuration accepted")
	}
}
