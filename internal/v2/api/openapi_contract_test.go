package api

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestOpenAPICoversEveryVersionedRouteAndMethod(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := yaml.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	expected := map[string][]string{
		"/resource-namespaces": {"get"},
		"/health/live":         {"get"}, "/health/ready": {"get"}, "/status": {"get"},
		"/resources": {"get"}, "/resource-types": {"get"}, "/resources/batch-get": {"post"}, "/selectors/resolve": {"post"},
		"/graph/resolve": {"post"}, "/branches/status": {"get"}, "/branches": {"get"}, "/ingest": {"post"},
		"/events/stream": {"get"}, "/business-processes": {"get"},
		"/business-processes/{name}":    {"get", "put", "delete"},
		"/live/resources/{id}/manifest": {"get"}, "/live/resources/{id}/metrics": {"get"},
		"/live/pods/{namespace}/{pod}/logs": {"get"}, "/live/metrics/query": {"post"},
		"/live/metrics/prometheus/{cluster}/api/v1/query":                {"get", "post"},
		"/live/metrics/prometheus/{cluster}/api/v1/query_range":          {"get", "post"},
		"/live/metrics/prometheus/{cluster}/api/v1/labels":               {"get", "post"},
		"/live/metrics/prometheus/{cluster}/api/v1/label/{label}/values": {"get"},
		"/live/metrics/prometheus/{cluster}/api/v1/series":               {"get", "post"},
		"/live/metrics/prometheus/{cluster}/api/v1/metadata":             {"get"},
		"/live/metrics/prometheus/{cluster}/api/v1/status/buildinfo":     {"get"},
	}
	seenOperationIDs := map[string]string{}
	federatedPaths := map[string]bool{
		"/resources": true, "/resource-types": true, "/selectors/resolve": true,
		"/live/resources/{id}/manifest": true, "/live/resources/{id}/metrics": true,
		"/live/pods/{namespace}/{pod}/logs": true, "/live/metrics/query": true,
		"/live/metrics/prometheus/{cluster}/api/v1/query":                true,
		"/live/metrics/prometheus/{cluster}/api/v1/query_range":          true,
		"/live/metrics/prometheus/{cluster}/api/v1/labels":               true,
		"/live/metrics/prometheus/{cluster}/api/v1/label/{label}/values": true,
		"/live/metrics/prometheus/{cluster}/api/v1/series":               true,
		"/live/metrics/prometheus/{cluster}/api/v1/metadata":             true,
		"/live/metrics/prometheus/{cluster}/api/v1/status/buildinfo":     true,
	}
	for path, methods := range expected {
		item, exists := document.Paths[path]
		if !exists {
			t.Errorf("OpenAPI path %s is missing", path)
			continue
		}
		for _, method := range methods {
			rawOperation, exists := item[method]
			if !exists {
				t.Errorf("OpenAPI operation %s %s is missing", strings.ToUpper(method), path)
				continue
			}
			var operation struct {
				OperationID string         `json:"operationId"`
				Responses   map[string]any `json:"responses"`
			}
			if err := json.Unmarshal(rawOperation, &operation); err != nil {
				t.Errorf("decode %s %s: %v", strings.ToUpper(method), path, err)
				continue
			}
			if operation.OperationID == "" {
				t.Errorf("%s %s has no operationId", strings.ToUpper(method), path)
			} else if previous, duplicate := seenOperationIDs[operation.OperationID]; duplicate {
				t.Errorf("duplicate operationId %q on %s and %s %s", operation.OperationID, previous, strings.ToUpper(method), path)
			} else {
				seenOperationIDs[operation.OperationID] = strings.ToUpper(method) + " " + path
			}
			if len(operation.Responses) == 0 {
				t.Errorf("%s %s has no response contract", strings.ToUpper(method), path)
			}
			if path != "/health/live" && path != "/health/ready" {
				for _, status := range []string{"401", "429"} {
					if _, exists := operation.Responses[status]; !exists {
						t.Errorf("%s %s does not document middleware response %s", strings.ToUpper(method), path, status)
					}
				}
			}
			if federatedPaths[path] {
				if _, exists := operation.Responses["508"]; !exists {
					t.Errorf("%s %s does not document the non-transitive federation guard", strings.ToUpper(method), path)
				}
			}
		}
	}
	for path, item := range document.Paths {
		methods, exists := expected[path]
		if !exists {
			t.Errorf("OpenAPI contains undocumented server route candidate %s", path)
			continue
		}
		allowed := map[string]bool{}
		for _, method := range methods {
			allowed[method] = true
		}
		for method := range item {
			if method == "parameters" || strings.HasPrefix(method, "x-") {
				continue
			}
			if !allowed[method] {
				t.Errorf("OpenAPI contains unregistered operation %s %s", strings.ToUpper(method), path)
			}
		}
	}
}

func TestOpenAPIDocumentsEventStreamResumeCursor(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	for _, expected := range []string{
		"name: Last-Event-ID",
		"in: header",
		"minimum: 0",
		"omitted starts at the current tail",
	} {
		if !strings.Contains(text, expected) {
			t.Errorf("OpenAPI event-stream cursor contract missing %q", expected)
		}
	}
}

func TestOpenAPIDocumentsBusinessProcessOptimisticConcurrency(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := yaml.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	var put struct {
		RequestBody struct {
			Content map[string]struct {
				Schema struct {
					Required []string `json:"required"`
				} `json:"schema"`
			} `json:"content"`
		} `json:"requestBody"`
		Responses map[string]any `json:"responses"`
	}
	if err := json.Unmarshal(document.Paths["/business-processes/{name}"]["put"], &put); err != nil {
		t.Fatal(err)
	}
	required := strings.Join(put.RequestBody.Content["application/json"].Schema.Required, ",")
	if !strings.Contains(required, "definition") || !strings.Contains(required, "expectedGeneration") {
		t.Fatalf("PUT required fields=%q", required)
	}
	if _, ok := put.Responses["409"]; !ok {
		t.Fatal("PUT does not document generation conflict")
	}
	var deleteOperation struct {
		Parameters []struct {
			Name     string `json:"name"`
			Required bool   `json:"required"`
		} `json:"parameters"`
		Responses map[string]any `json:"responses"`
	}
	if err := json.Unmarshal(document.Paths["/business-processes/{name}"]["delete"], &deleteOperation); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, parameter := range deleteOperation.Parameters {
		if parameter.Name == "expectedGeneration" && parameter.Required {
			found = true
		}
	}
	if !found {
		t.Fatal("DELETE does not require expectedGeneration")
	}
	if _, ok := deleteOperation.Responses["409"]; !ok {
		t.Fatal("DELETE does not document generation conflict")
	}
}

func TestOpenAPIResponsesHaveMachineReadableBodyContracts(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	paths, ok := document["paths"].(map[string]any)
	if !ok {
		t.Fatal("OpenAPI paths are missing")
	}
	for path, rawItem := range paths {
		item, ok := rawItem.(map[string]any)
		if !ok {
			t.Fatalf("path item %s is not an object", path)
		}
		for method, rawOperation := range item {
			if method == "parameters" || strings.HasPrefix(method, "x-") {
				continue
			}
			operation, ok := rawOperation.(map[string]any)
			if !ok {
				t.Fatalf("operation %s %s is not an object", strings.ToUpper(method), path)
			}
			responses, ok := operation["responses"].(map[string]any)
			if !ok || len(responses) == 0 {
				t.Errorf("%s %s has no response map", strings.ToUpper(method), path)
				continue
			}
			for status, rawResponse := range responses {
				response, err := resolveOpenAPIObject(document, rawResponse)
				if err != nil {
					t.Errorf("%s %s response %s: %v", strings.ToUpper(method), path, status, err)
					continue
				}
				// These responses deliberately have no entity body.
				if status == "204" || (status == "202" && path == "/ingest") ||
					(status == "200" && (path == "/health/live" || path == "/health/ready")) {
					continue
				}
				content, ok := response["content"].(map[string]any)
				if !ok || len(content) == 0 {
					t.Errorf("%s %s response %s has no media-type contract", strings.ToUpper(method), path, status)
					continue
				}
				for mediaType, rawMedia := range content {
					media, ok := rawMedia.(map[string]any)
					if !ok || media["schema"] == nil {
						t.Errorf("%s %s response %s media type %s has no schema", strings.ToUpper(method), path, status, mediaType)
					}
				}
				if len(status) == 3 && (status[0] == '4' || status[0] == '5') {
					jsonMedia, ok := content["application/json"].(map[string]any)
					if !ok || !referencesSchema(jsonMedia["schema"], "#/components/schemas/Error") {
						t.Errorf("%s %s response %s does not use the common JSON Error schema", strings.ToUpper(method), path, status)
					}
				}
			}
		}
	}
}

func resolveOpenAPIObject(document map[string]any, value any) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("response is not an object")
	}
	ref, referenced := object["$ref"].(string)
	if !referenced {
		return object, nil
	}
	var current any = document
	for _, segment := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		parent, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("reference %q does not resolve", ref)
		}
		current, ok = parent[segment]
		if !ok {
			return nil, fmt.Errorf("reference %q does not resolve", ref)
		}
	}
	resolved, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("reference %q is not an object", ref)
	}
	return resolved, nil
}

func referencesSchema(value any, expected string) bool {
	object, ok := value.(map[string]any)
	if !ok {
		return false
	}
	return object["$ref"] == expected
}
