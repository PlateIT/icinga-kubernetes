package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func configureAPICredentials(t *testing.T) {
	t.Helper()
	directory := t.TempDir()
	for _, role := range []string{"COLLECTOR", "READER", "FEDERATION", "ADMIN"} {
		path := filepath.Join(directory, role+"-token")
		if err := os.WriteFile(path, []byte("test-"+role+"-token"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("ICINGA_KUBERNETES_"+role+"_TOKEN_FILE", path)
	}
}

func TestRetentionDefaults(t *testing.T) {
	configureAPICredentials(t)
	t.Setenv("ICINGA_KUBERNETES_ROLE", "api")
	t.Setenv("ICINGA_KUBERNETES_CLUSTER_NAME", "test")
	t.Setenv("ICINGA_KUBERNETES_DATABASE_URL", "postgres://unused")
	t.Setenv("ICINGA_KUBERNETES_EVENT_RETENTION", "")
	t.Setenv("ICINGA_KUBERNETES_TOMBSTONE_RETENTION", "")
	t.Setenv("ICINGA_KUBERNETES_HISTORY_RETENTION", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EventRetention != 7*24*time.Hour {
		t.Fatalf("event retention=%s, want 168h", cfg.EventRetention)
	}
	if cfg.TombstoneRetention != 7*24*time.Hour {
		t.Fatalf("tombstone retention=%s, want 168h", cfg.TombstoneRetention)
	}
	if cfg.HistoryRetention != time.Hour {
		t.Fatalf("history retention=%s, want 1h", cfg.HistoryRetention)
	}
}

func TestLoadRejectsInvalidExplicitValues(t *testing.T) {
	configureAPICredentials(t)
	base := map[string]string{
		"ICINGA_KUBERNETES_ROLE": "api", "ICINGA_KUBERNETES_CLUSTER_NAME": "test", "ICINGA_KUBERNETES_DATABASE_URL": "postgres://unused",
	}
	for key, value := range base {
		t.Setenv(key, value)
	}
	for _, test := range []struct{ key, value string }{
		{"ICINGA_KUBERNETES_EVENT_RETENTION", "forever"},
		{"ICINGA_KUBERNETES_TOMBSTONE_RETENTION", "0s"},
		{"ICINGA_KUBERNETES_BATCH_SIZE", "1001"},
		{"ICINGA_KUBERNETES_RATE_BURST", "zero"},
		{"ICINGA_KUBERNETES_SPOOL_MAX_BYTES", "-1"},
		{"ICINGA_KUBERNETES_METRICS_INSECURE", "perhaps"},
	} {
		t.Run(test.key, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil {
				t.Fatalf("%s=%q was accepted", test.key, test.value)
			}
		})
	}
}

func TestLoadRejectsInvalidFreshnessOrder(t *testing.T) {
	configureAPICredentials(t)
	t.Setenv("ICINGA_KUBERNETES_ROLE", "api")
	t.Setenv("ICINGA_KUBERNETES_CLUSTER_NAME", "test")
	t.Setenv("ICINGA_KUBERNETES_DATABASE_URL", "postgres://unused")
	t.Setenv("ICINGA_KUBERNETES_STALE_AFTER", "2m")
	t.Setenv("ICINGA_KUBERNETES_UNAVAILABLE_AFTER", "30s")
	if _, err := Load(); err == nil {
		t.Fatal("stale threshold after unavailable threshold was accepted")
	}
}

func TestFederationTargetsAreStrictAndCredentialFree(t *testing.T) {
	valid, err := parseFederationTargets("campus=https://campus.example/api,local=http://api.svc:8080")
	if err != nil || len(valid) != 2 || valid[0].Name != "campus" {
		t.Fatalf("valid targets: %#v, %v", valid, err)
	}
	for _, invalid := range []string{
		"missing-url", "=https://example", "bad/name=https://example", "campus=ftp://example", "campus=https://user:pass@example",
		"campus=https://example?token=secret", "campus=https://one,campus=https://two", "campus=https://one,CAMPUS=https://two",
		strings.Repeat("n", 254) + "=https://example",
		"campus=https://example/" + strings.Repeat("x", 2048),
	} {
		if _, err := parseFederationTargets(invalid); err == nil {
			t.Errorf("invalid targets accepted: %s", invalid)
		}
	}
	many := make([]string, 65)
	for index := range many {
		many[index] = fmt.Sprintf("cluster-%d=https://cluster-%d.example", index, index)
	}
	if _, err := parseFederationTargets(strings.Join(many, ",")); err == nil {
		t.Fatal("more than 64 federation targets were accepted")
	}
}

func TestLoadRejectsFederatingToItself(t *testing.T) {
	configureAPICredentials(t)
	t.Setenv("ICINGA_KUBERNETES_ROLE", "api")
	t.Setenv("ICINGA_KUBERNETES_CLUSTER_NAME", "local")
	t.Setenv("ICINGA_KUBERNETES_DATABASE_URL", "postgres://unused")
	t.Setenv("ICINGA_KUBERNETES_FEDERATION_TARGETS", "local=https://local.example")
	if _, err := Load(); err == nil {
		t.Fatal("self-federation was accepted")
	}
}

func TestLoadRejectsMissingRoleCredentials(t *testing.T) {
	t.Setenv("ICINGA_KUBERNETES_ROLE", "api")
	t.Setenv("ICINGA_KUBERNETES_CLUSTER_NAME", "test")
	t.Setenv("ICINGA_KUBERNETES_DATABASE_URL", "postgres://unused")
	if _, err := Load(); err == nil {
		t.Fatal("API without role credentials was accepted")
	}

	t.Setenv("ICINGA_KUBERNETES_ROLE", "collector")
	t.Setenv("ICINGA_KUBERNETES_DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("collector without collector credential was accepted")
	}
}

func TestLoadRejectsMissingOrEmptyCredentialFile(t *testing.T) {
	t.Setenv("ICINGA_KUBERNETES_ROLE", "collector")
	t.Setenv("ICINGA_KUBERNETES_CLUSTER_NAME", "test")
	t.Setenv("ICINGA_KUBERNETES_COLLECTOR_TOKEN_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := Load(); err == nil {
		t.Fatal("missing token file was accepted")
	}
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ICINGA_KUBERNETES_COLLECTOR_TOKEN_FILE", empty)
	if _, err := Load(); err == nil {
		t.Fatal("empty token file was accepted")
	}
}

func TestLoadRejectsUnsafeLiveEndpointURLs(t *testing.T) {
	t.Setenv("ICINGA_KUBERNETES_ROLE", "collector")
	t.Setenv("ICINGA_KUBERNETES_CLUSTER_NAME", "test")
	t.Setenv("ICINGA_KUBERNETES_COLLECTOR_TOKEN", "collector-token")
	for key, value := range map[string]string{
		"ICINGA_KUBERNETES_API_URL":     "ftp://api.example",
		"ICINGA_KUBERNETES_METRICS_URL": "https://user:secret@metrics.example",
	} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, value)
			if _, err := Load(); err == nil {
				t.Fatalf("%s=%q was accepted", key, value)
			}
		})
	}
}

func TestLoadDoesNotInferMetricsCredentials(t *testing.T) {
	configureAPICredentials(t)
	t.Setenv("ICINGA_KUBERNETES_ROLE", "api")
	t.Setenv("ICINGA_KUBERNETES_CLUSTER_NAME", "test")
	t.Setenv("ICINGA_KUBERNETES_DATABASE_URL", "postgres://unused")
	t.Setenv("ICINGA_KUBERNETES_METRICS_URL", "https://metrics.example.test")
	t.Setenv("ICINGA_KUBERNETES_METRICS_TOKEN_FILE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MetricsTokenFile != "" {
		t.Fatalf("custom metrics URL inherited token file %q", cfg.MetricsTokenFile)
	}
}

func TestLoadValidatesNotificationsConfiguration(t *testing.T) {
	t.Setenv("ICINGA_KUBERNETES_ROLE", "worker")
	t.Setenv("ICINGA_KUBERNETES_CLUSTER_NAME", "test")
	t.Setenv("ICINGA_KUBERNETES_DATABASE_URL", "postgres://unused")
	t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_URL", "https://notifications.example.test")
	t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_WEB_URL", "https://icinga.example.test")
	t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_USERNAME", "icinga-kubernetes")
	t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_PASSWORD", "secret")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NotificationsURL != "https://notifications.example.test" || cfg.NotificationsUsername != "icinga-kubernetes" {
		t.Fatalf("notifications configuration not loaded: %#v", cfg)
	}

	for name, mutate := range map[string]func(){
		"missing web URL":  func() { t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_WEB_URL", "") },
		"relative web URL": func() { t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_WEB_URL", "/icingaweb2") },
		"non-worker role":  func() { t.Setenv("ICINGA_KUBERNETES_ROLE", "api"); configureAPICredentials(t) },
		"unsafe URL": func() {
			t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_URL", "https://user:secret@notifications.example.test")
		},
		"missing user": func() { t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_USERNAME", "") },
		"two passwords": func() {
			t.Setenv("ICINGA_KUBERNETES_NOTIFICATIONS_PASSWORD_FILE", filepath.Join(t.TempDir(), "password"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			mutate()
			if _, err := Load(); err == nil {
				t.Fatal("invalid notifications configuration was accepted")
			}
		})
	}
}
