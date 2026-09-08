package adapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type Definition struct {
	Name               string             `json:"name"`
	Group              string             `json:"group"`
	Kind               string             `json:"kind"`
	DescriptionFields  []string           `json:"descriptionFields,omitempty"`
	SummaryFields      []string           `json:"summaryFields,omitempty"`
	HealthyConditions  []string           `json:"healthyConditions,omitempty"`
	CriticalConditions []string           `json:"criticalConditions,omitempty"`
	CriticalPhases     []string           `json:"criticalPhases,omitempty"`
	StatusFields       []StatusField      `json:"statusFields,omitempty"`
	Metrics            []MetricDefinition `json:"metrics,omitempty"`
}

type Document struct {
	Version  int          `json:"version"`
	Adapters []Definition `json:"adapters"`
}

type StatusField struct {
	Path     string   `json:"path"`
	Healthy  []string `json:"healthy,omitempty"`
	Critical []string `json:"critical,omitempty"`
}

type MetricDefinition struct {
	Key   string `json:"key"`
	Label string `json:"label"`
	Unit  string `json:"unit"`
	Query string `json:"query"`
}

type Registry struct {
	mu          sync.RWMutex
	definitions []Definition
}

func New() *Registry { return &Registry{definitions: builtins()} }

func (r *Registry) Load(path string) error {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var document Document
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return fmt.Errorf("decode adapters: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("decode adapters: trailing JSON content")
	}
	if document.Version != 1 {
		return fmt.Errorf("unsupported adapter document version %d", document.Version)
	}
	custom := document.Adapters
	definitions := builtins()
	seenNames := map[string]struct{}{}
	seenMatches := map[string]struct{}{}
	for _, definition := range definitions {
		seenNames[strings.ToLower(definition.Name)] = struct{}{}
	}
	for _, d := range custom {
		if d.Name == "" || d.Kind == "" {
			return fmt.Errorf("adapter name and kind are required")
		}
		nameKey := strings.ToLower(d.Name)
		if _, exists := seenNames[nameKey]; exists {
			return fmt.Errorf("duplicate adapter name %q", d.Name)
		}
		seenNames[nameKey] = struct{}{}
		matchKey := strings.ToLower(d.Group + "\x00" + d.Kind)
		if _, exists := seenMatches[matchKey]; exists {
			return fmt.Errorf("duplicate adapter match for %s/%s", d.Group, d.Kind)
		}
		seenMatches[matchKey] = struct{}{}
		if err := validateDefinition(d); err != nil {
			return err
		}
		merged := false
		if d.Group != "*" && d.Kind != "*" {
			for index := range definitions {
				if definitions[index].Group == d.Group && strings.EqualFold(definitions[index].Kind, d.Kind) {
					mergedDefinition := mergeDefinition(definitions[index], d)
					if err := validateDefinition(mergedDefinition); err != nil {
						return fmt.Errorf("merged adapter %s: %w", d.Name, err)
					}
					definitions[index] = mergedDefinition
					merged = true
					break
				}
			}
		}
		if !merged {
			definitions = append(definitions, d)
		}
	}
	r.mu.Lock()
	r.definitions = definitions
	r.mu.Unlock()
	return nil
}

func validateDefinition(d Definition) error {
	if len(d.SummaryFields) > 32 || len(d.DescriptionFields) > 8 || len(d.StatusFields) > 8 {
		return fmt.Errorf("adapter %s exceeds field limits", d.Name)
	}
	if len(d.HealthyConditions) > 16 || len(d.CriticalConditions) > 16 || len(d.CriticalPhases) > 16 {
		return fmt.Errorf("adapter %s exceeds state rule limits", d.Name)
	}
	for _, path := range append(append([]string{}, d.DescriptionFields...), d.SummaryFields...) {
		if path == "" || len(path) > 256 || sensitivePath(path) {
			return fmt.Errorf("adapter %s contains an invalid or sensitive projection path %q", d.Name, path)
		}
	}
	for _, field := range d.StatusFields {
		if field.Path == "" || len(field.Path) > 256 || sensitivePath(field.Path) || len(field.Healthy) > 16 || len(field.Critical) > 16 {
			return fmt.Errorf("adapter %s contains an invalid status field", d.Name)
		}
	}
	if len(d.Metrics) > 16 {
		return fmt.Errorf("adapter %s exceeds metric limit", d.Name)
	}
	metricKeys := map[string]bool{}
	for _, metric := range d.Metrics {
		if !metricKeyPattern.MatchString(metric.Key) || metric.Label == "" || len(metric.Label) > 128 || metric.Unit == "" || len(metric.Unit) > 64 || metric.Query == "" || len(metric.Query) > 4096 {
			return fmt.Errorf("adapter %s contains an invalid metric", d.Name)
		}
		if metricKeys[metric.Key] {
			return fmt.Errorf("adapter %s contains duplicate metric key %q", d.Name, metric.Key)
		}
		metricKeys[metric.Key] = true
		clean := metric.Query
		for _, placeholder := range []string{"{{cluster}}", "{{namespace}}", "{{name}}", "{{kind}}"} {
			clean = strings.ReplaceAll(clean, placeholder, "")
		}
		if strings.Contains(clean, "{{") || strings.Contains(clean, "}}") {
			return fmt.Errorf("adapter %s metric %s contains an unsupported placeholder", d.Name, metric.Key)
		}
	}
	return nil
}

func sensitivePath(path string) bool {
	lower := strings.ToLower(path)
	for _, part := range []string{"token", "secret", "password", "credential", "authorization", "private-key", "privatekey"} {
		if strings.Contains(lower, part) {
			return true
		}
	}
	return false
}

var metricKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func mergeDefinition(base, extension Definition) Definition {
	base.Name = extension.Name
	base.DescriptionFields = append(base.DescriptionFields, extension.DescriptionFields...)
	base.SummaryFields = append(base.SummaryFields, extension.SummaryFields...)
	base.HealthyConditions = append(base.HealthyConditions, extension.HealthyConditions...)
	base.CriticalConditions = append(base.CriticalConditions, extension.CriticalConditions...)
	base.CriticalPhases = append(base.CriticalPhases, extension.CriticalPhases...)
	base.StatusFields = append(base.StatusFields, extension.StatusFields...)
	base.Metrics = append(base.Metrics, extension.Metrics...)
	return base
}

func (r *Registry) Metrics(group, kind string) []MetricDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	bestScore := -1
	var selected []MetricDefinition
	for _, definition := range r.definitions {
		if score := matchScore(definition, group, kind); score > bestScore {
			bestScore = score
			selected = definition.Metrics
		}
	}
	return append([]MetricDefinition(nil), selected...)
}

func (r *Registry) Describe(gvr schema.GroupVersionResource, obj *unstructured.Unstructured, conditions []model.Condition) (model.State, string, map[string]any) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	selected := Definition{Name: "generic", Group: "*", Kind: "*", HealthyConditions: []string{"Ready", "Available", "Healthy", "Synced"}, CriticalPhases: []string{"Failed", "Error"}, SummaryFields: []string{"status.phase", "status.replicas", "status.readyReplicas", "status.availableReplicas", "spec.replicas"}}
	bestScore := -1
	for _, d := range r.definitions {
		if score := matchScore(d, gvr.Group, obj.GetKind()); score > bestScore {
			bestScore = score
			selected = d
		}
	}
	summary := map[string]any{"adapter": selected.Name}
	for _, path := range append(selected.DescriptionFields, selected.SummaryFields...) {
		parts := strings.Split(path, ".")
		if v, ok, _ := unstructured.NestedFieldNoCopy(obj.Object, parts...); ok {
			summary[path] = v
		}
	}
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
	if selected.Name == "generic" {
		if state, reason, handled := describeNative(gvr.Group, obj, conditions, summary); handled {
			return state, reason, summary
		}
	}
	for _, bad := range selected.CriticalPhases {
		if strings.EqualFold(phase, bad) {
			return model.StateCritical, phase, summary
		}
	}
	for _, c := range conditions {
		for _, critical := range selected.CriticalConditions {
			if c.Type == critical && strings.EqualFold(c.Status, "True") {
				return model.StateCritical, c.Type + ": " + c.Reason, summary
			}
		}
		for _, healthy := range selected.HealthyConditions {
			if c.Type == healthy && strings.EqualFold(c.Status, "False") {
				return model.StateCritical, c.Type + ": " + c.Reason, summary
			}
		}
	}
	if selected.Name == "openshift-route" {
		for _, c := range conditions {
			if c.Type == "Admitted" && strings.EqualFold(c.Status, "True") {
				return model.StateOK, "Route admitted by router", summary
			}
		}
		return model.StateUnknown, "Route admission has not been confirmed", summary
	}
	statusObserved := false
	warningReason := ""
	for _, field := range selected.StatusFields {
		value, ok, _ := unstructured.NestedString(obj.Object, strings.Split(field.Path, ".")...)
		if !ok || value == "" {
			continue
		}
		statusObserved = true
		summary[field.Path] = value
		if containsFold(field.Critical, value) {
			return model.StateCritical, field.Path + ": " + value, summary
		}
		if warningReason == "" && len(field.Healthy) > 0 && !containsFold(field.Healthy, value) {
			warningReason = field.Path + ": " + value
		}
	}
	if warningReason != "" {
		return model.StateWarning, warningReason, summary
	}
	if phase != "" || len(conditions) > 0 || statusObserved {
		return model.StateOK, "", summary
	}
	return model.StateUnknown, "no status exposed by adapter", summary
}

func matchScore(definition Definition, group, kind string) int {
	if group == "" {
		group = "core"
	}
	if definition.Group != "*" && definition.Group != group {
		return -1
	}
	if definition.Kind != "*" && !strings.EqualFold(definition.Kind, kind) {
		return -1
	}
	score := 0
	if definition.Group != "*" {
		score += 2
	}
	if definition.Kind != "*" {
		score++
	}
	return score
}

func containsFold(values []string, candidate string) bool {
	for _, value := range values {
		if strings.EqualFold(value, candidate) {
			return true
		}
	}
	return false
}

func builtins() []Definition {
	return []Definition{
		{Name: "kubernetes-core-event", Group: "core", Kind: "Event", SummaryFields: []string{"type", "reason", "message", "count", "involvedObject.uid", "involvedObject.apiVersion", "involvedObject.fieldPath", "involvedObject.kind", "involvedObject.namespace", "involvedObject.name", "reportingComponent", "reportingInstance"}, StatusFields: []StatusField{{Path: "type", Healthy: []string{"Normal"}}}},
		{Name: "kubernetes-events-event", Group: "events.k8s.io", Kind: "Event", SummaryFields: []string{"type", "reason", "note", "regarding.uid", "regarding.apiVersion", "regarding.fieldPath", "regarding.kind", "regarding.namespace", "regarding.name", "reportingController", "reportingInstance", "deprecatedCount"}, StatusFields: []StatusField{{Path: "type", Healthy: []string{"Normal"}}}},
		{Name: "openshift-route", Group: "route.openshift.io", Kind: "Route", DescriptionFields: []string{"spec.host"}, HealthyConditions: []string{"Admitted"}},
		{Name: "openshift-cluster-operator", Group: "config.openshift.io", Kind: "ClusterOperator", HealthyConditions: []string{"Available"}, CriticalConditions: []string{"Degraded"}},
		{Name: "openshift-machine-config-pool", Group: "machineconfiguration.openshift.io", Kind: "MachineConfigPool", HealthyConditions: []string{"Updated"}, CriticalConditions: []string{"Degraded"}},
		{Name: "openshift-build", Group: "build.openshift.io", Kind: "Build", SummaryFields: []string{"status.phase"}, CriticalPhases: []string{"Failed", "Error", "Cancelled"}},
		{Name: "openshift-build-config", Group: "build.openshift.io", Kind: "BuildConfig"},
		{Name: "openshift-deployment-config", Group: "apps.openshift.io", Kind: "DeploymentConfig", HealthyConditions: []string{"Available"}},
		{Name: "openshift-image-stream", Group: "image.openshift.io", Kind: "ImageStream"},
		{Name: "olm-subscription", Group: "operators.coreos.com", Kind: "Subscription", CriticalConditions: []string{"CatalogSourcesUnhealthy"}},
		{Name: "olm-install-plan", Group: "operators.coreos.com", Kind: "InstallPlan", SummaryFields: []string{"status.phase"}, CriticalPhases: []string{"Failed"}},
		{Name: "olm-csv", Group: "operators.coreos.com", Kind: "ClusterServiceVersion", SummaryFields: []string{"status.phase"}, CriticalPhases: []string{"Failed"}},
		{Name: "argocd-application", Group: "argoproj.io", Kind: "Application", DescriptionFields: []string{"spec.project"}, StatusFields: []StatusField{
			{Path: "status.health.status", Healthy: []string{"Healthy"}, Critical: []string{"Degraded", "Missing"}},
			{Path: "status.sync.status", Healthy: []string{"Synced"}},
		}},
		{Name: "external-secret", Group: "external-secrets.io", Kind: "ExternalSecret", HealthyConditions: []string{"Ready"}},
	}
}
