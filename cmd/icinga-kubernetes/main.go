package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/icinga/icinga-kubernetes/internal/v2/api"
	"github.com/icinga/icinga-kubernetes/internal/v2/collector"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/notifier"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
	"github.com/icinga/icinga-kubernetes/internal/v2/store"
	"github.com/icinga/icinga-kubernetes/internal/v2/telemetry"
	"github.com/icinga/icinga-kubernetes/internal/v2/worker"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("icinga kubernetes stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := telemetry.Setup(ctx, cfg.Role, cfg.ClusterName)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			slog.Warn("OpenTelemetry shutdown failed", "error", err)
		}
	}()
	slog.Info("starting icinga kubernetes", "role", cfg.Role, "cluster", cfg.ClusterName)
	if cfg.Role == "collector" {
		metrics := operational.New(cfg.Role)
		return operational.Run(ctx, cfg.ProbeListen, metrics, func(roleCtx context.Context) error {
			return collector.Run(roleCtx, cfg, metrics)
		})
	}
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	maxOpenConnections := 30
	if cfg.Role == "api" {
		// Keep concurrent inventory reads below the point where PostgreSQL random
		// I/O starves the independently connected worker replicas during a relist.
		maxOpenConnections = 16
	}
	db.SetMaxOpenConns(maxOpenConnections)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	if cfg.Role == "migrate" {
		return store.Migrate(ctx, db)
	}
	if err := store.CheckSchema(ctx, db); err != nil {
		return err
	}
	switch cfg.Role {
	case "api":
		return serve(ctx, cfg, db)
	case "worker":
		host, _ := os.Hostname()
		metrics := operational.New(cfg.Role)
		return operational.Run(ctx, cfg.ProbeListen, metrics, func(roleCtx context.Context) error {
			if cfg.NotificationsURL == "" {
				return worker.Run(roleCtx, cfg, store.Store{DB: db}, host, metrics)
			}
			return runWorkerAndNotifier(roleCtx, cfg, db, host, metrics)
		})
	default:
		return fmt.Errorf("unsupported role %s", cfg.Role)
	}
}

func runWorkerAndNotifier(ctx context.Context, cfg config.Config, db *sql.DB, identity string, metrics *operational.Metrics) error {
	roleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsCh := make(chan error, 2)
	go func() { errorsCh <- worker.Run(roleCtx, cfg, store.Store{DB: db}, identity, metrics) }()
	go func() { errorsCh <- notifier.Run(roleCtx, cfg, db, identity) }()
	first := <-errorsCh
	cancel()
	second := <-errorsCh
	if (first == nil || errors.Is(first, context.Canceled)) && second != nil && !errors.Is(second, context.Canceled) {
		return second
	}
	return first
}

func serve(ctx context.Context, cfg config.Config, db *sql.DB) error {
	apiServer, err := api.New(cfg, db)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Addr: cfg.Listen, Handler: apiServer.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 1 << 20,
	}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("api listening", "address", cfg.Listen)
		errCh <- httpServer.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
