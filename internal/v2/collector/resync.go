package collector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

var resyncIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type resyncDocument struct {
	Version  int             `json:"version"`
	Requests []resyncRequest `json:"requests"`
}

type resyncRequest struct {
	ID       string `json:"id"`
	Group    string `json:"group"`
	Version  string `json:"version"`
	Resource string `json:"resource"`
}

func (r resyncRequest) gvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: r.Group, Version: r.Version, Resource: r.Resource}
}

func loadResyncRequests(path string) ([]resyncRequest, error) {
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var document resyncDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode resync requests: %w", err)
	}
	if err := rejectTrailingJSON(decoder); err != nil {
		return nil, err
	}
	if document.Version != 1 {
		return nil, fmt.Errorf("unsupported resync document version %d", document.Version)
	}
	if len(document.Requests) > 32 {
		return nil, errors.New("resync document exceeds 32 requests")
	}
	seen := make(map[string]struct{}, len(document.Requests))
	for _, request := range document.Requests {
		groupInvalid := request.Group != "" && len(validation.IsDNS1123Subdomain(request.Group)) > 0
		if !resyncIDPattern.MatchString(request.ID) || groupInvalid ||
			len(validation.IsDNS1035Label(request.Version)) > 0 || len(validation.IsDNS1035Label(request.Resource)) > 0 {
			return nil, fmt.Errorf("invalid resync request %q", request.ID)
		}
		if _, duplicate := seen[request.ID]; duplicate {
			return nil, fmt.Errorf("duplicate resync request ID %q", request.ID)
		}
		seen[request.ID] = struct{}{}
	}
	return document.Requests, nil
}

func rejectTrailingJSON(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("decode resync requests: trailing JSON content")
	}
	return fmt.Errorf("decode resync requests: %w", err)
}
