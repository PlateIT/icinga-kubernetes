package v1

import (
	"github.com/icinga/icinga-go-library/types"
	schemav1 "github.com/icinga/icinga-kubernetes/pkg/schema/v1"
	kmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type warmupObject struct {
	id              types.UUID
	resourceVersion string
}

type warmupObjects map[string][]warmupObject

// unchangedSinceWarmup reports whether obj is identical to the version read from the database during warmup.
// A key is only considered once so later watch events are always processed normally.
func (objects warmupObjects) unchangedSinceWarmup(key string, obj kmetav1.Object) bool {
	warmed, exists := objects[key]
	if !exists {
		return false
	}

	delete(objects, key)

	id := schemav1.EnsureUUID(obj.GetUID())
	for _, candidate := range warmed {
		if candidate.id == id && candidate.resourceVersion != "" &&
			candidate.resourceVersion == obj.GetResourceVersion() {
			return true
		}
	}

	return false
}
