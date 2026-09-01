package model

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type State string

const (
	StateOK       State = "ok"
	StateWarning  State = "warning"
	StateCritical State = "critical"
	StateUnknown  State = "unknown"
)

type Owner struct {
	UID        string `json:"uid"`
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Controller bool   `json:"controller,omitempty"`
}

type Condition struct {
	Type               string     `json:"type"`
	Status             string     `json:"status"`
	Reason             string     `json:"reason,omitempty"`
	Message            string     `json:"message,omitempty"`
	LastTransitionTime *time.Time `json:"lastTransitionTime,omitempty"`
}

type Resource struct {
	ID              string            `json:"id"`
	Cluster         string            `json:"cluster"`
	UID             string            `json:"uid"`
	Group           string            `json:"group"`
	Version         string            `json:"version"`
	Kind            string            `json:"kind"`
	Namespace       string            `json:"namespace,omitempty"`
	Name            string            `json:"name"`
	ResourceVersion string            `json:"resourceVersion"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
	Owners          []Owner           `json:"owners,omitempty"`
	Conditions      []Condition       `json:"conditions,omitempty"`
	Summary         map[string]any    `json:"summary,omitempty"`
	State           State             `json:"state"`
	Reason          string            `json:"reason,omitempty"`
	ObservedAt      time.Time         `json:"observedAt"`
	DeletedAt       *time.Time        `json:"deletedAt,omitempty"`
	Freshness       string            `json:"freshness,omitempty"`
}

func (r *Resource) Normalize() {
	if r.ID == "" {
		r.ID = StableID(r.Cluster, r.UID)
	}
	if r.State == "" {
		r.State = StateUnknown
	}
	if r.ObservedAt.IsZero() {
		r.ObservedAt = time.Now().UTC()
	}
	if r.Labels == nil {
		r.Labels = map[string]string{}
	}
	if r.Annotations == nil {
		r.Annotations = map[string]string{}
	}
	if r.Summary == nil {
		r.Summary = map[string]any{}
	}
}

func StableID(cluster, uid string) string {
	b := sha256.Sum256([]byte(cluster + "\x00" + uid))
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

type IngestEvent struct {
	EventID  string   `json:"eventId"`
	Action   string   `json:"action"`
	Resource Resource `json:"resource"`
}

type IngestBatch struct {
	Cluster string        `json:"cluster"`
	Shard   string        `json:"shard"`
	Events  []IngestEvent `json:"events"`
}

func (b IngestBatch) Validate() error {
	if b.Cluster == "" || len(b.Cluster) > 253 {
		return fmt.Errorf("cluster must contain 1..253 characters")
	}
	if b.Shard == "" || len(b.Shard) > 512 {
		return fmt.Errorf("shard must contain 1..512 characters")
	}
	if len(b.Events) < 1 || len(b.Events) > 1000 {
		return fmt.Errorf("batch must contain 1..1000 events")
	}
	for index, event := range b.Events {
		if _, err := uuid.Parse(event.EventID); err != nil {
			return fmt.Errorf("event %d has an invalid eventId", index)
		}
		if event.Action != "upsert" && event.Action != "delete" && event.Action != "reconcile" && event.Action != "heartbeat" {
			return fmt.Errorf("event %d has an invalid action", index)
		}
		if err := event.Resource.validate(b.Cluster); err != nil {
			return fmt.Errorf("event %d resource: %w", index, err)
		}
	}
	return nil
}

func (r Resource) validate(cluster string) error {
	if r.Cluster != cluster {
		return fmt.Errorf("cluster does not match batch")
	}
	if r.UID == "" || len(r.UID) > 512 {
		return fmt.Errorf("uid must contain 1..512 characters")
	}
	if r.ID != StableID(cluster, r.UID) {
		return fmt.Errorf("id does not match stable cluster/uid identity")
	}
	for name, value := range map[string]string{
		"group": r.Group, "version": r.Version, "kind": r.Kind, "name": r.Name, "resourceVersion": r.ResourceVersion,
	} {
		if value == "" || len(value) > 512 {
			return fmt.Errorf("%s must contain 1..512 characters", name)
		}
	}
	if len(r.Namespace) > 253 || len(r.Labels) > 256 || len(r.Annotations) > 256 || len(r.Owners) > 64 || len(r.Conditions) > 64 {
		return fmt.Errorf("resource metadata exceeds limits")
	}
	for key, value := range r.Labels {
		if len(key) > 253 || len(value) > 256 {
			return fmt.Errorf("label %q exceeds limits", key)
		}
	}
	for key, value := range r.Annotations {
		lower := strings.ToLower(key)
		if len(key) > 253 || len(value) > 4096 || containsSensitiveName(lower) {
			return fmt.Errorf("annotation %q is unsafe or exceeds limits", key)
		}
	}
	for index, owner := range r.Owners {
		if owner.UID == "" || owner.Kind == "" || owner.Name == "" || len(owner.UID) > 512 || len(owner.APIVersion) > 512 || len(owner.Kind) > 512 || len(owner.Name) > 512 {
			return fmt.Errorf("owner %d is invalid or exceeds limits", index)
		}
	}
	for index, condition := range r.Conditions {
		if condition.Type == "" || condition.Status == "" || len(condition.Type) > 512 || len(condition.Status) > 512 || len(condition.Reason) > 1024 || len(condition.Message) > 4096 {
			return fmt.Errorf("condition %d is invalid or exceeds limits", index)
		}
	}
	if len(r.Reason) > 4096 {
		return fmt.Errorf("reason exceeds limits")
	}
	if summary, err := json.Marshal(r.Summary); err != nil || len(summary) > 64<<10 {
		return fmt.Errorf("summary is invalid or exceeds 64 KiB")
	}
	if r.State != StateOK && r.State != StateWarning && r.State != StateCritical && r.State != StateUnknown {
		return fmt.Errorf("invalid state")
	}
	if r.ObservedAt.IsZero() {
		return fmt.Errorf("observedAt is required")
	}
	return nil
}

func containsSensitiveName(value string) bool {
	for _, needle := range []string{"token", "secret", "password", "credential", "authorization", "private-key"} {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

type Selector struct {
	Cluster     string  `json:"cluster,omitempty"`
	Group       string  `json:"group,omitempty"`
	Version     string  `json:"version,omitempty"`
	Kind        string  `json:"kind,omitempty"`
	Namespace   string  `json:"namespace,omitempty"`
	Name        string  `json:"name,omitempty"`
	Labels      string  `json:"labels,omitempty"`
	OwnerUID    string  `json:"ownerUID,omitempty"`
	States      []State `json:"states,omitempty"`
	Aggregation string  `json:"aggregation,omitempty"`
}

func Marshal(v any) json.RawMessage { b, _ := json.Marshal(v); return b }
