// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aero-arc/aero-arc-conformance/internal/config"
	"github.com/aero-arc/aero-arc-conformance/internal/conformance"
	"github.com/urfave/cli/v3"
)

func main() {
	if err := newCommand().Run(context.Background(), os.Args); err != nil {
		slog.Error("aero-arc-conformance failed", "error", err)
		os.Exit(1)
	}
}

func newCommand() *cli.Command {
	return &cli.Command{
		Name:  "aero-arc-conformance",
		Usage: "run the Aero Arc Conformance service",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config-path",
				Value: "configs/config.yaml",
				Usage: "path to the configuration file",
			},
		},
		Action: runConformance,
	}
}

func runConformance(ctx context.Context, cmd *cli.Command) error {
	cfg, err := config.Load(cmd.String("config-path"))
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	level := map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError}[cfg.Logging.Level]
	var handler slog.Handler
	if cfg.Logging.Format == "text" {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	log := slog.New(handler)
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	service, err := conformance.New(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("initialize conformance: %w", err)
	}
	log.Info("conformance started", "management_address", cfg.Service.ManagementAddress, "grpc_address", cfg.Service.GRPCAddress, "registry_address", cfg.Registry.Address, "worker_id", cfg.Worker.ID)
	if err := service.Run(ctx); err != nil {
		return fmt.Errorf("run conformance: %w", err)
	}
	return nil
}
