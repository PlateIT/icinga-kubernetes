package v1

import (
	"testing"

	"github.com/google/uuid"
	"github.com/icinga/icinga-go-library/types"
	kmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
)

func TestWarmupObjectsUnchangedSinceWarmup(t *testing.T) {
	id := types.UUID{UUID: uuid.New()}

	tests := []struct {
		name     string
		warmed   warmupObject
		object   *kmetav1.PartialObjectMetadata
		expected bool
	}{
		{
			name:     "unchanged object",
			warmed:   warmupObject{id: id, resourceVersion: "42"},
			object:   objectMetadata(id.String(), "42"),
			expected: true,
		},
		{
			name:   "changed resource version",
			warmed: warmupObject{id: id, resourceVersion: "41"},
			object: objectMetadata(id.String(), "42"),
		},
		{
			name:   "replacement object",
			warmed: warmupObject{id: id, resourceVersion: "42"},
			object: objectMetadata(uuid.NewString(), "42"),
		},
		{
			name:   "legacy row without resource version",
			warmed: warmupObject{id: id},
			object: objectMetadata(id.String(), "42"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objects := warmupObjects{"default/example": {test.warmed}}
			if actual := objects.unchangedSinceWarmup("default/example", test.object); actual != test.expected {
				t.Fatalf("expected %v, got %v", test.expected, actual)
			}
			if _, exists := objects["default/example"]; exists {
				t.Fatal("warmup key was not consumed")
			}
		})
	}
}

func TestWarmupObjectsOnlySuppressesInitialEvent(t *testing.T) {
	id := types.UUID{UUID: uuid.New()}
	object := objectMetadata(id.String(), "42")
	objects := warmupObjects{
		"default/example": {{id: id, resourceVersion: "42"}},
	}

	if !objects.unchangedSinceWarmup("default/example", object) {
		t.Fatal("initial unchanged event was not suppressed")
	}
	if objects.unchangedSinceWarmup("default/example", object) {
		t.Fatal("later event was unexpectedly suppressed")
	}
}

func objectMetadata(uid, resourceVersion string) *kmetav1.PartialObjectMetadata {
	return &kmetav1.PartialObjectMetadata{ObjectMeta: kmetav1.ObjectMeta{
		Namespace:       "default",
		Name:            "example",
		UID:             ktypes.UID(uid),
		ResourceVersion: resourceVersion,
	}}
}
