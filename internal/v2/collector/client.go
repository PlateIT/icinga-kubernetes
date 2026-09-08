package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
)

type Sender struct {
	cfg        config.Config
	client     *http.Client
	replayMu   sync.Mutex
	metrics    *operational.Metrics
	spoolFiles int64
	spoolBytes int64
}

const maxIngestBatchBytes = (2 << 20) - (64 << 10)

func NewSender(cfg config.Config, provided ...*operational.Metrics) (*Sender, error) {
	metrics := operational.New("collector")
	if len(provided) > 0 && provided[0] != nil {
		metrics = provided[0]
	}
	sender := &Sender{cfg: cfg, client: &http.Client{Timeout: 30 * time.Second}, metrics: metrics}
	if err := os.MkdirAll(cfg.SpoolPath, 0700); err != nil {
		return nil, fmt.Errorf("initialize collector spool: %w", err)
	}
	files, bytes, err := sender.spoolUsage()
	if err != nil {
		return nil, fmt.Errorf("inspect collector spool: %w", err)
	}
	sender.spoolFiles = files
	sender.spoolBytes = bytes
	metrics.SetSpool(files, bytes)
	return sender, nil
}

func (s *Sender) Send(ctx context.Context, batch model.IngestBatch) error {
	chunks, err := splitBatch(batch, maxIngestBatchBytes)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		if err := s.sendChunk(ctx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sender) sendChunk(ctx context.Context, batch model.IngestBatch) error {
	if err := s.replay(ctx); err != nil {
		return s.spoolWithBackpressure(ctx, batch)
	}
	if err := s.post(ctx, batch); err != nil {
		return s.spoolWithBackpressure(ctx, batch)
	}
	return nil
}

func splitBatch(batch model.IngestBatch, limit int) ([]model.IngestBatch, error) {
	if len(batch.Events) == 0 {
		return nil, errors.New("cannot split an empty ingest batch")
	}
	empty, err := json.Marshal(model.IngestBatch{Cluster: batch.Cluster, Shard: batch.Shard, Events: []model.IngestEvent{}})
	if err != nil {
		return nil, fmt.Errorf("encode ingest batch envelope: %w", err)
	}
	base := len(empty) - 2 // exclude the empty JSON array brackets
	chunks := make([]model.IngestBatch, 0, 1)
	current := model.IngestBatch{Cluster: batch.Cluster, Shard: batch.Shard}
	currentBytes := base
	for index, event := range batch.Events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("encode ingest event %d: %w", index, err)
		}
		if base+len(encoded)+2 > limit {
			return nil, fmt.Errorf("ingest event %d exceeds the %d-byte request limit", index, limit)
		}
		separator := 0
		if len(current.Events) > 0 {
			separator = 1
		}
		if len(current.Events) > 0 && currentBytes+separator+len(encoded)+2 > limit {
			chunks = append(chunks, current)
			current = model.IngestBatch{Cluster: batch.Cluster, Shard: batch.Shard}
			currentBytes = base
			separator = 0
		}
		current.Events = append(current.Events, event)
		currentBytes += separator + len(encoded)
	}
	chunks = append(chunks, current)
	return chunks, nil
}

func (s *Sender) spoolWithBackpressure(ctx context.Context, batch model.IngestBatch) error {
	for {
		err := s.spool(batch)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errSpoolFull) {
			return err
		}
		s.metrics.Backpressure()
		// Stop consuming the watch until durable space is available. Kubernetes
		// may close the blocked watch; the collector then performs a full list.
		if err := s.replay(ctx); err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

var errSpoolFull = errors.New("collector spool capacity reached")

func (s *Sender) Reconcile(ctx context.Context, cluster, shard string, cutoff time.Time) error {
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(cluster+"\x00"+shard+"\x00reconcile\x00"+cutoff.Format(time.RFC3339Nano)))
	uid := "reconcile:" + shard
	r := model.Resource{ID: model.StableID(cluster, uid), Cluster: cluster, UID: uid, Group: "icinga-kubernetes.io", Version: "v2", Kind: "ReconcileMarker", Name: shard, ResourceVersion: cutoff.Format(time.RFC3339Nano), State: model.StateOK, ObservedAt: cutoff}
	return s.Send(ctx, model.IngestBatch{Cluster: cluster, Shard: shard, Events: []model.IngestEvent{{EventID: id.String(), Action: "reconcile", Resource: r}}})
}

func (s *Sender) Heartbeat(ctx context.Context, cluster string, observed time.Time) error {
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(cluster+"\x00heartbeat\x00"+observed.Format(time.RFC3339Nano)))
	const uid = "heartbeat"
	r := model.Resource{ID: model.StableID(cluster, uid), Cluster: cluster, UID: uid, Group: "icinga-kubernetes.io", Version: "v2", Kind: "Heartbeat", Name: "collector", ResourceVersion: observed.Format(time.RFC3339Nano), State: model.StateOK, ObservedAt: observed}
	return s.Send(ctx, model.IngestBatch{Cluster: cluster, Shard: "__heartbeat__", Events: []model.IngestEvent{{EventID: id.String(), Action: "heartbeat", Resource: r}}})
}

func (s *Sender) post(ctx context.Context, batch model.IngestBatch) error {
	b, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.cfg.APIURL, "/")+"/api/v1/ingest", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	token := s.cfg.CollectorToken
	if s.cfg.CollectorTokenFile != "" {
		if b, readErr := os.ReadFile(s.cfg.CollectorTokenFile); readErr == nil {
			token = strings.TrimSpace(string(b))
		}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := s.client.Do(req)
	if err != nil {
		s.metrics.Error()
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		s.metrics.Error()
		return fmt.Errorf("ingest API returned %s", resp.Status)
	}
	s.metrics.Success()
	return nil
}

func (s *Sender) spool(batch model.IngestBatch) error {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()
	if err := os.MkdirAll(s.cfg.SpoolPath, 0700); err != nil {
		return err
	}
	b, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	if s.spoolBytes+int64(len(b)) > s.cfg.SpoolMaxBytes {
		return errSpoolFull
	}
	name := time.Now().UTC().Format("20060102T150405.000000000") + "-" + uuid.NewString() + ".json"
	tmp := filepath.Join(s.cfg.SpoolPath, "."+name)
	dst := filepath.Join(s.cfg.SpoolPath, name)
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := file.Write(b); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	complete = true
	s.spoolFiles++
	s.spoolBytes += int64(len(b))
	s.metrics.Spooled(int64(len(b)))
	return nil
}

func (s *Sender) replay(ctx context.Context) error {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()
	entries, err := os.ReadDir(s.cfg.SpoolPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") && strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		path := filepath.Join(s.cfg.SpoolPath, name)
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var batch model.IngestBatch
		if err = json.Unmarshal(b, &batch); err != nil {
			return fmt.Errorf("invalid spool file %s: %w", name, err)
		}
		if err = s.post(ctx, batch); err != nil {
			return err
		}
		if err = os.Remove(path); err != nil {
			return err
		}
		s.spoolFiles--
		s.spoolBytes -= int64(len(b))
		s.metrics.Replayed(int64(len(b)))
	}
	return nil
}

func (s *Sender) spoolUsage() (files, bytes int64, err error) {
	entries, err := os.ReadDir(s.cfg.SpoolPath)
	if os.IsNotExist(err) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return 0, 0, statErr
		}
		files++
		bytes += info.Size()
	}
	return files, bytes, nil
}
