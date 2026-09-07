package notifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	icingaconfig "github.com/icinga/icinga-go-library/config"
	"github.com/icinga/icinga-go-library/notifications/event"
	"github.com/icinga/icinga-go-library/notifications/source"
	"github.com/icinga/icinga-go-library/types"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
)

const claimDuration = 30 * time.Second

type Record struct {
	ID         int64
	ResourceID string
	Cluster    string
	Group      string
	Version    string
	Kind       string
	Namespace  string
	Name       string
	Labels     map[string]string
	State      string
	Reason     string
	Action     string
	Attempts   int
}

type Store struct{ DB *sql.DB }

func (s Store) Claim(ctx context.Context, identity string) (Record, bool, error) {
	row := s.DB.QueryRowContext(ctx, `WITH candidate AS (
		SELECT event.id FROM notification_outbox event
		WHERE event.delivered_at IS NULL AND event.available_at <= clock_timestamp()
		  AND (event.claimed_until IS NULL OR event.claimed_until < clock_timestamp())
		  AND NOT EXISTS (
			SELECT 1 FROM notification_outbox earlier
			WHERE earlier.resource_id=event.resource_id AND earlier.id<event.id
			  AND earlier.delivered_at IS NULL
		  )
		ORDER BY event.id FOR UPDATE OF event SKIP LOCKED LIMIT 1
	)
	UPDATE notification_outbox event
	SET claimed_by=$1,claimed_until=clock_timestamp()+$2::interval,attempts=attempts+1
	FROM candidate WHERE event.id=candidate.id
	RETURNING event.id,event.resource_id,event.cluster_name,event.api_group,event.api_version,event.kind,
		event.namespace,event.name,event.labels,event.state,event.reason,event.action,event.attempts`, identity, durationSQL(claimDuration))
	var record Record
	var labels []byte
	if err := row.Scan(&record.ID, &record.ResourceID, &record.Cluster, &record.Group, &record.Version,
		&record.Kind, &record.Namespace, &record.Name, &labels, &record.State, &record.Reason,
		&record.Action, &record.Attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, false, nil
		}
		return Record{}, false, err
	}
	if err := json.Unmarshal(labels, &record.Labels); err != nil {
		return Record{}, false, fmt.Errorf("decode notification labels: %w", err)
	}
	return record, true, nil
}

func (s Store) Delivered(ctx context.Context, id int64, identity string) error {
	result, err := s.DB.ExecContext(ctx, `UPDATE notification_outbox
		SET delivered_at=clock_timestamp(),claimed_by=NULL,claimed_until=NULL,last_error=NULL
		WHERE id=$1 AND claimed_by=$2 AND delivered_at IS NULL`, id, identity)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return errors.New("notification outbox claim was lost before acknowledgement")
	}
	return nil
}

func (s Store) Retry(ctx context.Context, id int64, identity string, cause error) error {
	message := cause.Error()
	if len(message) > 4096 {
		message = message[:4096]
	}
	_, err := s.DB.ExecContext(ctx, `UPDATE notification_outbox
		SET available_at=clock_timestamp()+LEAST(interval '1 minute',power(2,LEAST(attempts,6))*interval '1 second'),
			claimed_by=NULL,claimed_until=NULL,last_error=$3
		WHERE id=$1 AND claimed_by=$2 AND delivered_at IS NULL`, id, identity, message)
	return err
}

func Run(ctx context.Context, cfg config.Config, db *sql.DB, identity string) error {
	sourceConfig := source.Config{
		Url: cfg.NotificationsURL, Username: cfg.NotificationsUsername,
		Password: cfg.NotificationsPassword, PasswordFile: cfg.NotificationsPasswordFile,
		TlsOptions: icingaconfig.TLS{TLSCommon: icingaconfig.TLSCommon{Ca: cfg.NotificationsCAFile}},
	}
	if err := sourceConfig.Validate(); err != nil {
		return fmt.Errorf("validate Icinga Notifications source: %w", err)
	}
	client, err := source.NewClient(sourceConfig, "Icinga Kubernetes v2")
	if err != nil {
		return fmt.Errorf("create Icinga Notifications client: %w", err)
	}
	store := Store{DB: db}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		record, found, err := store.Claim(ctx, identity)
		if err != nil {
			slog.Error("claim notification event failed", "error", err)
			timer.Reset(time.Second)
			continue
		}
		if !found {
			timer.Reset(250 * time.Millisecond)
			continue
		}
		ev := makeEvent(record, cfg.NotificationsWebURL)
		sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, sendErr := client.ProcessEvent(sendCtx, ev, false)
		cancel()
		if sendErr == nil {
			sendErr = store.Delivered(ctx, record.ID, identity)
		}
		if sendErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if retryErr := store.Retry(ctx, record.ID, identity, sendErr); retryErr != nil {
				slog.Error("release failed notification event failed", "event", record.ID, "error", retryErr)
			}
			slog.Warn("notification delivery failed", "event", record.ID, "attempt", record.Attempts, "error", sendErr)
			timer.Reset(time.Second)
			continue
		}
		timer.Reset(0)
	}
}

func makeEvent(record Record, webURL string) *event.Event {
	displayName := record.Name
	if record.Namespace != "" {
		displayName = record.Namespace + "/" + displayName
	}
	displayName = record.Kind + " " + displayName
	message := record.Reason
	if message == "" {
		message = "Kubernetes resource state is " + record.State
	}
	query := url.Values{"id": {record.ResourceID}, "cluster": {record.Cluster}}
	// Retries must keep the same UUID for server-side deduplication. Scope the
	// outbox sequence by cluster and resource to distinguish independent sources.
	identity, _ := json.Marshal([]any{"icinga-kubernetes", record.Cluster, record.ResourceID, record.ID})
	ev := &event.Event{
		ID: types.MakeUUID(uuid.NewSHA1(uuid.NameSpaceOID, identity)), Name: displayName,
		URL:      strings.TrimRight(webURL, "/") + "/kubernetes/resources/show?" + query.Encode(),
		Tags:     map[string]string{"cluster": record.Cluster, "resource_id": record.ResourceID},
		Severity: severity(record.State), Message: message,
		Incident: types.MakeBool(true),
		Relations: map[string]any{"kubernetes": map[string]any{
			"cluster": record.Cluster, "group": record.Group, "version": record.Version,
			"kind": record.Kind, "namespace": record.Namespace, "name": record.Name,
			"labels": record.Labels, "state": record.State, "reason": record.Reason,
		}},
		CompleteRelations: []string{"kubernetes"},
	}
	if record.Action == "delete" || record.State == "ok" {
		ev.Close = types.MakeBool(true)
	}
	return ev
}

func severity(state string) event.Severity {
	switch strings.ToLower(state) {
	case "ok":
		return event.SeverityOK
	case "critical":
		return event.SeverityCrit
	case "warning", "unknown":
		return event.SeverityWarning
	default:
		return event.SeverityNotice
	}
}

func durationSQL(duration time.Duration) string { return fmt.Sprintf("%f seconds", duration.Seconds()) }
