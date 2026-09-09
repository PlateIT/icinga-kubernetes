package adapter

import (
	"fmt"
	"strings"

	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

// Ready must be true; pressure and network-unavailable conditions must be
// false. Evaluate all conditions so Ready cannot hide a known failure.
func describeNode(conditions []model.Condition) (model.State, string) {
	var problems, unknown []string
	readySeen := false
	for _, kind := range []string{"Ready", "DiskPressure", "MemoryPressure", "PIDPressure", "NetworkUnavailable"} {
		for _, c := range conditions {
			if c.Type != kind {
				continue
			}
			readySeen = readySeen || kind == "Ready"
			detail := fmt.Sprintf("%s=%s", kind, c.Status)
			if c.Reason != "" {
				detail += ": " + c.Reason
			}
			if c.Message != "" {
				detail += ": " + c.Message
			}
			if c.Status != "True" && c.Status != "False" {
				unknown = append(unknown, detail)
			} else if (kind == "Ready" && c.Status == "False") || (kind != "Ready" && c.Status == "True") {
				problems = append(problems, detail)
			}
		}
	}
	if !readySeen {
		unknown = append(unknown, "Ready condition missing")
	}
	if len(problems) > 0 {
		return model.StateCritical, strings.Join(append(problems, unknown...), "; ")
	}
	if len(unknown) > 0 {
		return model.StateUnknown, strings.Join(unknown, "; ")
	}
	return model.StateOK, "Node ready; no reported pressure or network failure"
}
