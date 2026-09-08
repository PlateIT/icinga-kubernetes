package store

import (
	"strings"
	"testing"
)

func TestNamePatternAndEventFilterPreserveExactRestrictions(t *testing.T) {
	args, where, err := buildResourceFilter(ListFilter{Cluster: "test", Name: "allowed", NamePattern: "*WEB_%*", EventForUID: "pod-uid"}, false)
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.Join(where, " AND ")
	if !strings.Contains(sql, "r.name=$") || !strings.Contains(sql, "lower(r.name) LIKE") || !strings.Contains(sql, "r.kind='Event'") {
		t.Fatal(sql)
	}
	found := false
	for _, arg := range args {
		if arg == `%web\_\%%` {
			found = true
		}
	}
	if !found {
		t.Fatalf("SQL wildcards were not escaped: %v", args)
	}
}
