package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
	"github.com/icinga/icinga-kubernetes/internal/v2/store"
)

type eventStore interface {
	ApplyShard(context.Context, string, int) (int, error)
	Cleanup(context.Context, time.Duration, time.Duration, time.Duration, time.Duration) error
}

type intervals struct {
	cleanup time.Duration
	idle    time.Duration
	retry   time.Duration
}

var productionIntervals = intervals{cleanup: 10 * time.Minute, idle: 250 * time.Millisecond, retry: time.Second}

const applyShardSize = 8

func Run(ctx context.Context, cfg config.Config, s store.Store, identity string, metrics *operational.Metrics) error {
	return run(ctx, cfg, s, identity, metrics, productionIntervals)
}

func run(ctx context.Context, cfg config.Config, s eventStore, identity string, metrics *operational.Metrics, waits intervals) error {
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	cleanup := time.NewTicker(waits.cleanup)
	defer cleanup.Stop()
	idle := time.NewTimer(0)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cleanup.C:
			if err := s.Cleanup(ctx, cfg.EventRetention, cfg.TombstoneRetention, cfg.HistoryRetention, cfg.UnavailableAfter); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				metrics.Error()
				slog.Error("cleanup failed", "error", err)
			}
		case <-idle.C:
			processed, err := s.ApplyShard(ctx, identity, applyShardSize)
			if processed > 0 {
				metrics.Processed(uint64(processed))
			}
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				metrics.Error()
				slog.Error("ingest event failed", "error", err)
				if processed > 0 {
					idle.Reset(0)
				} else {
					idle.Reset(waits.retry)
				}
				continue
			}
			if processed > 0 {
				idle.Reset(0)
			} else {
				idle.Reset(waits.idle)
			}
		}
	}
}
