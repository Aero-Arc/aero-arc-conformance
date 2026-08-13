// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"flag"
	"github.com/aero-arc/aero-arc-conformance/internal/app"
	"github.com/aero-arc/aero-arc-conformance/internal/config"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	path := flag.String("config-path", "configs/config.yaml", "configuration file")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err != nil {
		slog.Error("load configuration", "error", err)
		os.Exit(1)
	}
	level := map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError}[cfg.Logging.Level]
	var handler slog.Handler
	if cfg.Logging.Format == "text" {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	log := slog.New(handler)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	application, err := app.New(ctx, cfg, log)
	if err != nil {
		log.Error("initialize conformance", "error", err)
		os.Exit(1)
	}
	log.Info("conformance started", "management_address", cfg.Service.ManagementAddress, "worker_id", cfg.Worker.ID)
	if err = application.Run(ctx); err != nil {
		log.Error("conformance stopped", "error", err)
		os.Exit(1)
	}
}
