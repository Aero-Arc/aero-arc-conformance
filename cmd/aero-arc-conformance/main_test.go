// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandHelp(t *testing.T) {
	var output bytes.Buffer
	command := newCommand()
	command.Writer = &output

	if err := command.Run(context.Background(), []string{"aero-arc-conformance", "--help"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !strings.Contains(output.String(), "--config-path") {
		t.Fatalf("help output does not describe --config-path: %q", output.String())
	}
}

func TestCommandReportsConfigurationLoadError(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "missing.yaml")
	err := newCommand().Run(context.Background(), []string{"aero-arc-conformance", "--config-path", missingPath})
	if err == nil {
		t.Fatal("Run() error = nil, want configuration load error")
	}
	if !strings.Contains(err.Error(), "load configuration") {
		t.Fatalf("Run() error = %q, want configuration load context", err)
	}
}
