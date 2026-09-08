package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

type Store struct{ DB *sql.DB }

var ErrGenerationConflict = errors.New("generation conflict")

type ListFilter struct {
	Cluster, Group, Version, Kind, Namespace, Name, NamePrefix string
	Labels                                                     map[string]string
	States                                                     []model.State
	OwnerUID                                                   string
	Limit                                                      int
	Cursor                                                     string
	OrderByID                                                  bool
	HideZeroReplicaSets                                        bool
}

type Page struct {
	Items      []model.Resource `json:"items"`
	NextCursor string           `json:"nextCursor,omitempty"`
	Snapshot   time.Time        `json:"snapshot"`
	Freshness  string           `json:"freshness"`
}

type ResourceType struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
	Count   int64  `json:"count"`
}

func (s Store) ResourceTypes(ctx context.Context, cluster string, filters ...ListFilter) ([]ResourceType, error) {
	if cluster == "" {
		return nil, errors.New("cluster is required")
	}
	f := ListFilter{Cluster: cluster}
	if len(filters) > 0 {
		f = filters[0]
		f.Cluster = cluster
	}
	args, where, err := buildResourceFilter(f, false)
	if err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT r.api_group,r.api_version,r.kind,count(*)
		FROM resource r WHERE `+strings.Join(where, " AND ")+`
		GROUP BY r.api_group,r.api_version,r.kind ORDER BY lower(r.kind),r.api_group,r.api_version LIMIT 4097`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]ResourceType, 0)
	for rows.Next() {
		var item ResourceType
		if err := rows.Scan(&item.Group, &item.Version, &item.Kind, &item.Count); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(items) > 4096 {
		return nil, errors.New("resource type inventory exceeds 4096 entries")
	}
	return items, nil
}

type SelectorResolution struct {
	Items    []model.Resource
	Matched  int
	Counts   map[model.State]int
	Snapshot time.Time
}

func (s Store) PutFederationCache(ctx context.Context, endpoint, key string, status int, response []byte, freshness string) error {
	if s.DB == nil {
		return errors.New("database unavailable")
	}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO federation_cache(endpoint,cache_key,status_code,response,freshness,fetched_at)
		VALUES($1,$2,$3,$4,$5,clock_timestamp()) ON CONFLICT(endpoint,cache_key) DO UPDATE SET
		status_code=excluded.status_code,response=excluded.response,freshness=excluded.freshness,fetched_at=excluded.fetched_at`, endpoint, key, status, response, freshness)
	return err
}

func (s Store) GetFederationCache(ctx context.Context, endpoint, key string) (int, []byte, string, time.Time, error) {
	if s.DB == nil {
		return 0, nil, "", time.Time{}, errors.New("database unavailable")
	}
	var status int
	var response []byte
	var freshness string
	var fetched time.Time
	err := s.DB.QueryRowContext(ctx, `SELECT status_code,response,freshness,fetched_at FROM federation_cache WHERE endpoint=$1 AND cache_key=$2`, endpoint, key).Scan(&status, &response, &freshness, &fetched)
	return status, response, freshness, fetched, err
}

type BusinessProcess struct {
	Name       string          `json:"name"`
	Definition json.RawMessage `json:"definition,omitempty"`
	Generation int64           `json:"generation"`
	UpdatedAt  time.Time       `json:"updatedAt"`
}

type Change struct {
	Sequence   int64     `json:"sequence"`
	ResourceID string    `json:"resourceId"`
	Cluster    string    `json:"cluster"`
	Action     string    `json:"action"`
	ChangedAt  time.Time `json:"changedAt"`
}

func (s Store) LatestSequence(ctx context.Context) (int64, error) {
	var sequence int64
	err := s.DB.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0) FROM change_log`).Scan(&sequence)
	return sequence, err
}
func (s Store) Changes(ctx context.Context, after int64, limit int) ([]Change, error) {
	if limit < 1 || limit > 1000 {
		limit = 250
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT sequence,resource_id,cluster_name,action,changed_at FROM change_log WHERE sequence>$1 ORDER BY sequence LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Change{}
	for rows.Next() {
		var c Change
		if err := rows.Scan(&c.Sequence, &c.ResourceID, &c.Cluster, &c.Action, &c.ChangedAt); err != nil {
			return nil, err
		}
		items = append(items, c)
	}
	return items, rows.Err()
}

func (s Store) ListBusinessProcesses(ctx context.Context) ([]BusinessProcess, error) {
	rows, err := s.DB.QueryContext(ctx, `/*NO LOAD BALANCE*/ SELECT name,generation,updated_at FROM business_process ORDER BY lower(name),name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []BusinessProcess{}
	for rows.Next() {
		var p BusinessProcess
		if err := rows.Scan(&p.Name, &p.Generation, &p.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, p)
	}
	return items, rows.Err()
}
func (s Store) GetBusinessProcess(ctx context.Context, name string) (BusinessProcess, error) {
	var p BusinessProcess
	err := s.DB.QueryRowContext(ctx, `/*NO LOAD BALANCE*/ SELECT name,definition,generation,updated_at FROM business_process WHERE name=$1`, name).Scan(&p.Name, &p.Definition, &p.Generation, &p.UpdatedAt)
	return p, err
}
func (s Store) PutBusinessProcess(ctx context.Context, name string, definition json.RawMessage, expectedGeneration int64) (BusinessProcess, error) {
	var p BusinessProcess
	var err error
	if expectedGeneration == 0 {
		err = s.DB.QueryRowContext(ctx, `INSERT INTO business_process(name,definition) VALUES($1,$2)
			ON CONFLICT(name) DO NOTHING
			RETURNING name,definition,generation,updated_at`, name, definition).
			Scan(&p.Name, &p.Definition, &p.Generation, &p.UpdatedAt)
	} else {
		err = s.DB.QueryRowContext(ctx, `UPDATE business_process
			SET definition=$2,generation=generation+1,updated_at=clock_timestamp()
			WHERE name=$1 AND generation=$3
			RETURNING name,definition,generation,updated_at`, name, definition, expectedGeneration).
			Scan(&p.Name, &p.Definition, &p.Generation, &p.UpdatedAt)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return BusinessProcess{}, ErrGenerationConflict
	}
	return p, err
}
func (s Store) DeleteBusinessProcess(ctx context.Context, name string, expectedGeneration int64) (bool, error) {
	var result string
	err := s.DB.QueryRowContext(ctx, `WITH deleted AS (
		DELETE FROM business_process WHERE name=$1 AND generation=$2 RETURNING 1
	)
	SELECT CASE
		WHEN EXISTS (SELECT 1 FROM deleted) THEN 'deleted'
		WHEN EXISTS (SELECT 1 FROM business_process WHERE name=$1) THEN 'conflict'
		ELSE 'missing'
	END`, name, expectedGeneration).Scan(&result)
	if err != nil {
		return false, err
	}
	if result == "conflict" {
		return false, ErrGenerationConflict
	}
	return result == "deleted", nil
}

func (s Store) Enqueue(ctx context.Context, batch model.IngestBatch) error {
	if err := batch.Validate(); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var insertedCount int64
	var firstInsertedID string
	for _, event := range batch.Events {
		payload, err := json.Marshal(event.Resource)
		if err != nil {
			return err
		}
		result, insertErr := tx.ExecContext(ctx, `INSERT INTO ingest_event(event_id, cluster_name, shard, action, payload)
			VALUES ($1,$2,$3,$4,$5) ON CONFLICT (event_id) DO NOTHING`, event.EventID, batch.Cluster, batch.Shard, event.Action, payload)
		if insertErr != nil {
			return insertErr
		}
		inserted, insertErr := result.RowsAffected()
		if insertErr != nil {
			return insertErr
		}
		if inserted == 1 && firstInsertedID == "" {
			firstInsertedID = event.EventID
		}
		insertedCount += inserted
	}
	if insertedCount > 0 {
		if _, err = tx.ExecContext(ctx, `INSERT INTO ingest_shard_head(cluster_name,shard,event_id,pending_events)
			VALUES($1,$2,$3,$4) ON CONFLICT(cluster_name,shard) DO UPDATE
			SET event_id=COALESCE(ingest_shard_head.event_id,excluded.event_id),
				pending_events=ingest_shard_head.pending_events+excluded.pending_events`,
			batch.Cluster, batch.Shard, firstInsertedID, insertedCount); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE ingest_shard_head SET event_id=(
			SELECT event_id FROM ingest_event WHERE cluster_name=$1 AND shard=$2 AND applied_at IS NULL
			ORDER BY received_at,event_id LIMIT 1) WHERE cluster_name=$1 AND shard=$2`, batch.Cluster, batch.Shard); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type claimedEvent struct {
	id      string
	cluster string
	action  string
	shard   string
	payload []byte
}

func (s Store) ApplyShard(ctx context.Context, _ string, limit int) (int, error) {
	if limit < 1 || limit > 64 {
		return 0, fmt.Errorf("apply shard limit must be between 1 and 64")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var cluster, shard string
	err = tx.QueryRowContext(ctx, `SELECT event.cluster_name,event.shard
		FROM ingest_shard_head head JOIN ingest_event event ON event.event_id=head.event_id
		WHERE event.applied_at IS NULL AND event.available_at <= clock_timestamp()
		ORDER BY event.received_at,event.event_id FOR UPDATE OF head SKIP LOCKED LIMIT 1`).Scan(&cluster, &shard)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT event_id,action,shard,payload FROM ingest_event
		WHERE cluster_name=$1 AND shard=$2 AND applied_at IS NULL AND available_at <= clock_timestamp()
		ORDER BY received_at,event_id LIMIT $3`, cluster, shard, limit)
	if err != nil {
		return 0, err
	}
	events := make([]claimedEvent, 0, limit)
	for rows.Next() {
		var event claimedEvent
		if err = rows.Scan(&event.id, &event.action, &event.shard, &event.payload); err != nil {
			_ = rows.Close()
			return 0, err
		}
		event.cluster = cluster
		events = append(events, event)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	completed := make([]string, 0, len(events))
	var firstFailure error
	for _, event := range events {
		var resource model.Resource
		if decodeErr := json.Unmarshal(event.payload, &resource); decodeErr != nil {
			if _, updateErr := tx.ExecContext(ctx, `UPDATE ingest_event SET error=$2,attempts=attempts+1,
				available_at=clock_timestamp() + LEAST(interval '1 minute', (attempts + 1) * interval '1 second') WHERE event_id=$1`, event.id, decodeErr.Error()); updateErr != nil {
				return 0, updateErr
			}
			firstFailure = decodeErr
			break
		}
		resource.Normalize()
		if err = applyResource(ctx, tx, event.action, event.shard, resource); err != nil {
			return 0, err
		}
		completed = append(completed, event.id)
	}
	if len(completed) > 0 {
		marks := make([]string, len(completed))
		args := make([]any, len(completed))
		for i, id := range completed {
			marks[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}
		if _, err = tx.ExecContext(ctx, `UPDATE ingest_event SET applied_at=clock_timestamp(),error=NULL
			WHERE event_id IN (`+strings.Join(marks, ",")+`)`, args...); err != nil {
			return 0, err
		}
	}
	var nextID string
	err = tx.QueryRowContext(ctx, `SELECT event_id FROM ingest_event
		WHERE cluster_name=$1 AND shard=$2 AND applied_at IS NULL
		ORDER BY received_at,event_id LIMIT 1`, cluster, shard).Scan(&nextID)
	var next any = nextID
	if errors.Is(err, sql.ErrNoRows) {
		next = nil
		err = nil
	}
	if err == nil && len(completed) > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE ingest_shard_head
			SET event_id=$3,pending_events=pending_events-$4,last_applied_at=clock_timestamp()
			WHERE cluster_name=$1 AND shard=$2`, cluster, shard, next, len(completed))
	}
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(completed), firstFailure
}

func applyResource(ctx context.Context, tx *sql.Tx, action, shard string, r model.Resource) error {
	if action == "heartbeat" {
		return nil
	}
	if action == "reconcile" {
		_, err := tx.ExecContext(ctx, `WITH removed AS (
			UPDATE resource SET deleted_at=clock_timestamp(),observed_at=$3,changed_at=clock_timestamp()
			WHERE cluster_name=$1 AND shard=$2 AND deleted_at IS NULL AND observed_at < $3
			RETURNING id,cluster_name,api_group,api_version,kind,namespace,name,labels,state,reason
		), watermarked AS (
			UPDATE resource SET observed_at=$3
			WHERE cluster_name=$1 AND shard=$2 AND deleted_at IS NOT NULL AND observed_at < $3
			RETURNING id
		), queued AS (
			INSERT INTO notification_outbox(resource_id,cluster_name,api_group,api_version,kind,namespace,name,labels,state,reason,action)
			SELECT id,cluster_name,api_group,api_version,kind,namespace,name,labels,state,reason,'delete'
			FROM removed WHERE state <> 'ok'
			RETURNING resource_id,cluster_name
		), changed AS (
			INSERT INTO change_log(resource_id,cluster_name,action)
			SELECT id,cluster_name,'delete' FROM removed
			RETURNING resource_id
		)
		SELECT (SELECT count(*) FROM queued)+(SELECT count(*) FROM changed)+(SELECT count(*) FROM watermarked)`, r.Cluster, shard, r.ObservedAt)
		return err
	}
	labels, _ := json.Marshal(r.Labels)
	annotations, _ := json.Marshal(r.Annotations)
	conditions, _ := json.Marshal(r.Conditions)
	summary, _ := json.Marshal(r.Summary)
	var previousState, previousReason sql.NullString
	var previousDeletedAt sql.NullTime
	var previousObservedAt time.Time
	var relationsUnchanged bool
	previousExists := true
	if err := tx.QueryRowContext(ctx, `SELECT state,reason,deleted_at,observed_at,
		labels=$2::jsonb AND api_group=$3 AND api_version=$4 AND kind=$5 AND namespace=$6 AND name=$7
		FROM resource WHERE id=$1 FOR UPDATE`, r.ID, labels, r.Group, r.Version, r.Kind, r.Namespace, r.Name).
		Scan(&previousState, &previousReason, &previousDeletedAt, &previousObservedAt, &relationsUnchanged); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		previousExists = false
	}
	// A former leader may replay its durable spool after a new leader has
	// completed a newer snapshot. Never let that older observation resurrect
	// or roll back the resource.
	if previousExists && r.ObservedAt.Before(previousObservedAt) {
		return nil
	}
	deletedAt := r.DeletedAt
	if action == "delete" && deletedAt == nil {
		now := time.Now().UTC()
		deletedAt = &now
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO resource(id,cluster_name,shard,kubernetes_uid,api_group,api_version,kind,namespace,name,
		resource_version,labels,annotations,conditions,summary,state,reason,observed_at,deleted_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		ON CONFLICT(id) DO UPDATE SET shard=excluded.shard,kubernetes_uid=excluded.kubernetes_uid,
		api_group=excluded.api_group,api_version=excluded.api_version,kind=excluded.kind,
		namespace=excluded.namespace,name=excluded.name,resource_version=excluded.resource_version,labels=excluded.labels,
		annotations=excluded.annotations,conditions=excluded.conditions,summary=excluded.summary,state=excluded.state,
		reason=excluded.reason,observed_at=excluded.observed_at,deleted_at=excluded.deleted_at,changed_at=clock_timestamp()`,
		r.ID, r.Cluster, shard, r.UID, r.Group, r.Version, r.Kind, r.Namespace, r.Name, r.ResourceVersion, labels, annotations, conditions, summary, r.State, r.Reason, r.ObservedAt, deletedAt)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM resource_owner WHERE resource_id=$1`, r.ID); err != nil {
		return err
	}
	if action != "delete" {
		for _, owner := range r.Owners {
			if _, err = tx.ExecContext(ctx, `INSERT INTO resource_owner(resource_id,owner_uid,api_version,kind,name,controller) VALUES($1,$2,$3,$4,$5,$6)`, r.ID, owner.UID, owner.APIVersion, owner.Kind, owner.Name, owner.Controller); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO change_log(resource_id,cluster_name,action) VALUES($1,$2,$3)`, r.ID, r.Cluster, action); err != nil {
		return err
	}
	shouldNotify := action == "delete" && previousExists && !previousDeletedAt.Valid && previousState.String != string(model.StateOK)
	if action != "delete" {
		if !previousExists {
			shouldNotify = r.State != model.StateOK
		} else if previousDeletedAt.Valid {
			shouldNotify = r.State != model.StateOK
		} else {
			shouldNotify = previousState.String != string(r.State) ||
				(r.State != model.StateOK && (previousReason.String != r.Reason || !relationsUnchanged))
		}
	}
	if !shouldNotify {
		return nil
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO notification_outbox(
		resource_id,cluster_name,api_group,api_version,kind,namespace,name,labels,state,reason,action)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, r.ID, r.Cluster, r.Group, r.Version,
		r.Kind, r.Namespace, r.Name, labels, r.State, r.Reason, action)
	return err
}

func (s Store) List(ctx context.Context, f ListFilter) (Page, error) {
	if f.Limit < 1 {
		f.Limit = 100
	}
	if f.Limit > 1000 {
		f.Limit = 1000
	}
	args, where, err := buildResourceFilter(f, f.NamePrefix == "")
	if err != nil {
		return Page{}, err
	}
	order := "r.sort_id"
	if f.OrderByID {
		order = "r.id"
	} else if f.NamePrefix != "" {
		order = "lower(r.name),r.sort_id"
		if f.Cursor != "" {
			name, sortID, cursorErr := decodeNameCursor(f.Cursor)
			if cursorErr != nil {
				return Page{}, cursorErr
			}
			args = append(args, name, sortID)
			where = append(where, fmt.Sprintf("(lower(r.name),r.sort_id) > ($%d,$%d)", len(args)-1, len(args)))
		}
	}
	args = append(args, f.Limit+1)
	q := `SELECT r.id,r.cluster_name,r.kubernetes_uid,r.api_group,r.api_version,r.kind,r.namespace,r.name,r.resource_version,
		r.labels,r.annotations,r.conditions,r.summary,r.state,r.reason,r.observed_at,r.deleted_at,r.sort_id
		FROM resource r WHERE ` + strings.Join(where, " AND ") + fmt.Sprintf(" ORDER BY %s LIMIT $%d", order, len(args))
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	p := Page{Items: []model.Resource{}, Snapshot: time.Now().UTC(), Freshness: "live"}
	sortIDs := make([]int64, 0, f.Limit+1)
	for rows.Next() {
		var sortID int64
		r, err := scanResource(rows, &sortID)
		if err != nil {
			return Page{}, err
		}
		p.Items = append(p.Items, r)
		sortIDs = append(sortIDs, sortID)
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}
	if len(p.Items) > f.Limit {
		last := strconv.FormatInt(sortIDs[f.Limit-1], 10)
		if f.OrderByID {
			last = p.Items[f.Limit-1].ID
		} else if f.NamePrefix != "" {
			encoded, marshalErr := json.Marshal([]any{strings.ToLower(p.Items[f.Limit-1].Name), sortIDs[f.Limit-1]})
			if marshalErr != nil {
				return Page{}, marshalErr
			}
			last = string(encoded)
		}
		p.Items = p.Items[:f.Limit]
		p.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(last))
	}
	return p, nil
}

func decodeNameCursor(cursor string) (string, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", 0, errors.New("invalid cursor")
	}
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) != nil || len(values) != 2 {
		return "", 0, errors.New("invalid cursor")
	}
	var name string
	var sortID int64
	if json.Unmarshal(values[0], &name) != nil || json.Unmarshal(values[1], &sortID) != nil || sortID < 1 || len(name) > 512 {
		return "", 0, errors.New("invalid cursor")
	}
	return name, sortID, nil
}

func buildResourceFilter(f ListFilter, includeCursor bool) ([]any, []string, error) {
	args := []any{}
	where := []string{"r.deleted_at IS NULL"}
	if f.HideZeroReplicaSets {
		where = append(where, "NOT (r.api_group='apps' AND r.kind='ReplicaSet' AND COALESCE(r.summary->>'spec.replicas','')='0' AND COALESCE(r.summary->>'status.readyReplicas','0')='0' AND COALESCE(r.summary->>'status.availableReplicas','0')='0')")
	}
	add := func(column, value string) {
		if value != "" {
			args = append(args, value)
			where = append(where, fmt.Sprintf("%s=$%d", column, len(args)))
		}
	}
	add("r.cluster_name", f.Cluster)
	add("r.api_group", f.Group)
	add("r.api_version", f.Version)
	add("r.kind", f.Kind)
	add("r.namespace", f.Namespace)
	add("r.name", f.Name)
	if f.NamePrefix != "" {
		prefix := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(strings.ToLower(f.NamePrefix))
		args = append(args, prefix+"%")
		where = append(where, fmt.Sprintf(`lower(r.name) LIKE $%d ESCAPE '\'`, len(args)))
	}
	if len(f.Labels) > 0 {
		b, _ := json.Marshal(f.Labels)
		args = append(args, b)
		where = append(where, fmt.Sprintf("r.labels @> $%d::jsonb", len(args)))
	}
	if len(f.States) > 0 {
		vals := make([]string, len(f.States))
		for i, v := range f.States {
			vals[i] = string(v)
		}
		args = append(args, strings.Join(vals, ","))
		where = append(where, fmt.Sprintf("r.state = ANY(string_to_array($%d, ','))", len(args)))
	}
	if f.OwnerUID != "" {
		args = append(args, f.OwnerUID)
		where = append(where, fmt.Sprintf("EXISTS(SELECT 1 FROM resource_owner o WHERE o.resource_id=r.id AND o.owner_uid=$%d)", len(args)))
	}
	if includeCursor && f.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(f.Cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid cursor")
		}
		if f.OrderByID {
			id, parseErr := uuid.Parse(string(raw))
			if parseErr != nil {
				return nil, nil, fmt.Errorf("invalid cursor")
			}
			args = append(args, id.String())
			where = append(where, fmt.Sprintf("r.id > $%d", len(args)))
			return args, where, nil
		}
		sortID, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil || sortID < 1 {
			return nil, nil, fmt.Errorf("invalid cursor")
		}
		args = append(args, sortID)
		where = append(where, fmt.Sprintf("r.sort_id > $%d", len(args)))
	}
	return args, where, nil
}

type rowScanner interface{ Scan(...any) error }

func scanResource(row rowScanner, extra ...any) (model.Resource, error) {
	var r model.Resource
	var labels, annotations, conditions, summary []byte
	destinations := []any{&r.ID, &r.Cluster, &r.UID, &r.Group, &r.Version, &r.Kind, &r.Namespace, &r.Name, &r.ResourceVersion, &labels, &annotations, &conditions, &summary, &r.State, &r.Reason, &r.ObservedAt, &r.DeletedAt}
	destinations = append(destinations, extra...)
	if err := row.Scan(destinations...); err != nil {
		return model.Resource{}, err
	}
	if err := json.Unmarshal(labels, &r.Labels); err != nil {
		return model.Resource{}, fmt.Errorf("decode resource labels: %w", err)
	}
	if err := json.Unmarshal(annotations, &r.Annotations); err != nil {
		return model.Resource{}, fmt.Errorf("decode resource annotations: %w", err)
	}
	if err := json.Unmarshal(conditions, &r.Conditions); err != nil {
		return model.Resource{}, fmt.Errorf("decode resource conditions: %w", err)
	}
	if err := json.Unmarshal(summary, &r.Summary); err != nil {
		return model.Resource{}, fmt.Errorf("decode resource summary: %w", err)
	}
	return r, nil
}

func (s Store) ResolveSelector(ctx context.Context, f ListFilter) (SelectorResolution, error) {
	args, where, err := buildResourceFilter(f, false)
	if err != nil {
		return SelectorResolution{}, err
	}
	args = append(args, 1000)
	query := `/*NO LOAD BALANCE*/ SELECT r.id,r.cluster_name,r.kubernetes_uid,r.api_group,r.api_version,r.kind,r.namespace,r.name,r.resource_version,
		r.labels,r.annotations,r.conditions,r.summary,r.state,r.reason,r.observed_at,r.deleted_at,
		count(*) OVER (),
		count(*) FILTER (WHERE r.state='ok') OVER (),
		count(*) FILTER (WHERE r.state='warning') OVER (),
		count(*) FILTER (WHERE r.state='critical') OVER (),
		count(*) FILTER (WHERE r.state='unknown') OVER ()
		FROM resource r WHERE ` + strings.Join(where, " AND ") + fmt.Sprintf(" ORDER BY r.id LIMIT $%d", len(args))
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return SelectorResolution{}, err
	}
	defer rows.Close()
	result := SelectorResolution{Items: []model.Resource{}, Counts: map[model.State]int{}, Snapshot: time.Now().UTC()}
	for rows.Next() {
		var matched, okCount, warningCount, criticalCount, unknownCount int
		resource, scanErr := scanResource(rows, &matched, &okCount, &warningCount, &criticalCount, &unknownCount)
		if scanErr != nil {
			return SelectorResolution{}, scanErr
		}
		result.Items = append(result.Items, resource)
		result.Matched = matched
		result.Counts[model.StateOK] = okCount
		result.Counts[model.StateWarning] = warningCount
		result.Counts[model.StateCritical] = criticalCount
		result.Counts[model.StateUnknown] = unknownCount
	}
	if err := rows.Err(); err != nil {
		return SelectorResolution{}, err
	}
	return result, nil
}

func (s Store) GetIDs(ctx context.Context, ids []string) ([]model.Resource, error) {
	if len(ids) == 0 {
		return []model.Resource{}, nil
	}
	if len(ids) > 1000 {
		return nil, fmt.Errorf("at most 1000 ids")
	}
	args := make([]any, len(ids))
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		args[i] = id
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	rows, err := s.DB.QueryContext(ctx, `/*NO LOAD BALANCE*/ SELECT r.id,r.cluster_name,r.kubernetes_uid,r.api_group,r.api_version,r.kind,r.namespace,r.name,r.resource_version,
		r.labels,r.annotations,r.conditions,r.summary,r.state,r.reason,r.observed_at,r.deleted_at
		FROM resource r WHERE r.deleted_at IS NULL AND r.id IN (`+strings.Join(placeholders, ",")+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []model.Resource{}
	for rows.Next() {
		r, err := scanResource(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, r)
	}
	return items, rows.Err()
}

func (s Store) ChildrenMany(ctx context.Context, ids []string) (map[string][]model.Resource, error) {
	parents, err := s.GetIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	result := map[string][]model.Resource{}
	byUID := map[string]string{}
	for _, p := range parents {
		result[p.ID] = []model.Resource{}
		byUID[p.UID] = p.ID
	}
	if len(byUID) == 0 {
		return result, nil
	}
	uids := make([]string, 0, len(byUID))
	for uid := range byUID {
		uids = append(uids, uid)
	}
	args := make([]any, len(uids))
	marks := make([]string, len(uids))
	for i, uid := range uids {
		args[i] = uid
		marks[i] = fmt.Sprintf("$%d", i+1)
	}
	rows, err := s.DB.QueryContext(ctx, `/*NO LOAD BALANCE*/ SELECT r.id,r.cluster_name,r.kubernetes_uid,r.api_group,r.api_version,r.kind,r.namespace,r.name,r.resource_version,r.labels,r.annotations,r.conditions,r.summary,r.state,r.reason,r.observed_at,r.deleted_at,o.owner_uid FROM resource_owner o JOIN resource r ON r.id=o.resource_id WHERE r.deleted_at IS NULL AND o.owner_uid IN (`+strings.Join(marks, ",")+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var owner string
		r, err := scanResource(rows, &owner)
		if err != nil {
			return nil, err
		}
		result[byUID[owner]] = append(result[byUID[owner]], r)
	}
	return result, rows.Err()
}

func (s Store) Status(ctx context.Context) (map[string]any, error) {
	var pending int64
	var oldest, last sql.NullTime
	err := s.DB.QueryRowContext(ctx, `SELECT COALESCE(sum(head.pending_events),0),min(event.received_at),max(head.last_applied_at)
		FROM ingest_shard_head head
		LEFT JOIN ingest_event event ON event.event_id=head.event_id
		`).Scan(&pending, &oldest, &last)
	if err != nil {
		return nil, err
	}
	var notificationPending, notificationFailures int64
	var oldestNotification sql.NullTime
	if err := s.DB.QueryRowContext(ctx, `SELECT count(*),min(created_at),count(*) FILTER (WHERE last_error IS NOT NULL)
		FROM notification_outbox WHERE delivered_at IS NULL`).Scan(&notificationPending, &oldestNotification, &notificationFailures); err != nil {
		return nil, err
	}
	var oldestValue, lastValue any
	if oldest.Valid {
		oldestValue = oldest.Time
	}
	if last.Valid {
		lastValue = last.Time
	}
	var oldestNotificationValue any
	if oldestNotification.Valid {
		oldestNotificationValue = oldestNotification.Time
	}
	return map[string]any{
		"pendingEvents": pending, "oldestPendingEvent": oldestValue, "lastAppliedAt": lastValue,
		"pendingNotifications": notificationPending, "failedNotifications": notificationFailures,
		"oldestPendingNotification": oldestNotificationValue,
	}, nil
}

func (s Store) Cleanup(ctx context.Context, eventRetention, tombstoneRetention, historyRetention, federationCacheRetention time.Duration) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin retention cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	deletes := []struct {
		name      string
		statement string
		retention time.Duration
	}{
		{"events", `DELETE FROM resource WHERE lower(kind)='event' AND api_group IN ('core','events.k8s.io') AND observed_at < clock_timestamp()-$1::interval`, eventRetention},
		{"tombstones", `DELETE FROM resource WHERE deleted_at < clock_timestamp()-$1::interval`, tombstoneRetention},
		{"history", `DELETE FROM change_log WHERE changed_at < clock_timestamp()-$1::interval`, historyRetention},
		{"delivered notifications", `DELETE FROM notification_outbox WHERE delivered_at IS NOT NULL AND delivered_at < clock_timestamp()-$1::interval`, historyRetention},
		{"shard metadata", `DELETE FROM ingest_shard_head WHERE pending_events=0 AND last_applied_at < clock_timestamp()-$1::interval`, historyRetention},
		{"inbox", `DELETE FROM ingest_event WHERE applied_at < clock_timestamp()-$1::interval`, historyRetention},
		{"federation cache", `DELETE FROM federation_cache WHERE fetched_at < clock_timestamp()-$1::interval`, federationCacheRetention},
	}
	for _, deletion := range deletes {
		if _, err = tx.ExecContext(ctx, deletion.statement, durationSQL(deletion.retention)); err != nil {
			return fmt.Errorf("delete expired %s: %w", deletion.name, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit retention cleanup: %w", err)
	}
	return nil
}

func durationSQL(d time.Duration) string { return fmt.Sprintf("%f seconds", d.Seconds()) }
