package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type FederationTarget struct {
	Name string
	URL  string
}

type Config struct {
	Role                      string
	Listen                    string
	ProbeListen               string
	DatabaseURL               string
	ClusterName               string
	APIURL                    string
	CollectorToken            string
	ReaderToken               string
	FederationToken           string
	AdminToken                string
	CollectorTokenFile        string
	ReaderTokenFile           string
	FederationTokenFile       string
	AdminTokenFile            string
	FederationTargets         []FederationTarget
	SpoolPath                 string
	LeaseNamespace            string
	Identity                  string
	AdapterFile               string
	ResyncFile                string
	MetricsURL                string
	MetricsNamespace          string
	MetricsTokenFile          string
	MetricsCAFile             string
	MetricsInsecure           bool
	NotificationsURL          string
	NotificationsWebURL       string
	NotificationsUsername     string
	NotificationsPassword     string
	NotificationsPasswordFile string
	NotificationsCAFile       string
	EventRetention            time.Duration
	TombstoneRetention        time.Duration
	HistoryRetention          time.Duration
	StaleAfter                time.Duration
	UnavailableAfter          time.Duration
	BatchSize                 int
	SpoolMaxBytes             int64
	RatePerSecond             int
	RateBurst                 int
}

var federationNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

func Load() (Config, error) {
	if err := validateExplicitEnvironment(); err != nil {
		return Config{}, err
	}
	c := Config{
		Role:                      value("ICINGA_KUBERNETES_ROLE", "api"),
		Listen:                    value("ICINGA_KUBERNETES_LISTEN", ":8080"),
		ProbeListen:               value("ICINGA_KUBERNETES_PROBE_LISTEN", ":8081"),
		DatabaseURL:               os.Getenv("ICINGA_KUBERNETES_DATABASE_URL"),
		ClusterName:               os.Getenv("ICINGA_KUBERNETES_CLUSTER_NAME"),
		APIURL:                    value("ICINGA_KUBERNETES_API_URL", "http://icinga-kubernetes-api:8080"),
		CollectorToken:            os.Getenv("ICINGA_KUBERNETES_COLLECTOR_TOKEN"),
		ReaderToken:               os.Getenv("ICINGA_KUBERNETES_READER_TOKEN"),
		FederationToken:           os.Getenv("ICINGA_KUBERNETES_FEDERATION_TOKEN"),
		AdminToken:                os.Getenv("ICINGA_KUBERNETES_ADMIN_TOKEN"),
		CollectorTokenFile:        os.Getenv("ICINGA_KUBERNETES_COLLECTOR_TOKEN_FILE"),
		ReaderTokenFile:           os.Getenv("ICINGA_KUBERNETES_READER_TOKEN_FILE"),
		FederationTokenFile:       os.Getenv("ICINGA_KUBERNETES_FEDERATION_TOKEN_FILE"),
		AdminTokenFile:            os.Getenv("ICINGA_KUBERNETES_ADMIN_TOKEN_FILE"),
		SpoolPath:                 value("ICINGA_KUBERNETES_SPOOL_PATH", "/var/lib/icinga-kubernetes/spool"),
		LeaseNamespace:            value("ICINGA_KUBERNETES_LEASE_NAMESPACE", value("POD_NAMESPACE", "icinga")),
		Identity:                  value("ICINGA_KUBERNETES_IDENTITY", value("HOSTNAME", "icinga-kubernetes")),
		AdapterFile:               value("ICINGA_KUBERNETES_ADAPTER_FILE", "/etc/icinga-kubernetes/adapters.json"),
		ResyncFile:                value("ICINGA_KUBERNETES_RESYNC_FILE", "/etc/icinga-kubernetes/resync.json"),
		MetricsURL:                strings.TrimRight(os.Getenv("ICINGA_KUBERNETES_METRICS_URL"), "/"),
		MetricsNamespace:          os.Getenv("ICINGA_KUBERNETES_METRICS_NAMESPACE"),
		MetricsTokenFile:          os.Getenv("ICINGA_KUBERNETES_METRICS_TOKEN_FILE"),
		MetricsCAFile:             os.Getenv("ICINGA_KUBERNETES_METRICS_CA_FILE"),
		MetricsInsecure:           boolean("ICINGA_KUBERNETES_METRICS_INSECURE", false),
		NotificationsURL:          strings.TrimRight(os.Getenv("ICINGA_KUBERNETES_NOTIFICATIONS_URL"), "/"),
		NotificationsWebURL:       strings.TrimRight(os.Getenv("ICINGA_KUBERNETES_NOTIFICATIONS_WEB_URL"), "/"),
		NotificationsUsername:     os.Getenv("ICINGA_KUBERNETES_NOTIFICATIONS_USERNAME"),
		NotificationsPassword:     os.Getenv("ICINGA_KUBERNETES_NOTIFICATIONS_PASSWORD"),
		NotificationsPasswordFile: os.Getenv("ICINGA_KUBERNETES_NOTIFICATIONS_PASSWORD_FILE"),
		NotificationsCAFile:       os.Getenv("ICINGA_KUBERNETES_NOTIFICATIONS_CA_FILE"),
		EventRetention:            duration("ICINGA_KUBERNETES_EVENT_RETENTION", 7*24*time.Hour),
		TombstoneRetention:        duration("ICINGA_KUBERNETES_TOMBSTONE_RETENTION", 7*24*time.Hour),
		HistoryRetention:          duration("ICINGA_KUBERNETES_HISTORY_RETENTION", time.Hour),
		StaleAfter:                duration("ICINGA_KUBERNETES_STALE_AFTER", 30*time.Second),
		UnavailableAfter:          duration("ICINGA_KUBERNETES_UNAVAILABLE_AFTER", 2*time.Minute),
		BatchSize:                 integer("ICINGA_KUBERNETES_BATCH_SIZE", 250),
		SpoolMaxBytes:             integer64("ICINGA_KUBERNETES_SPOOL_MAX_BYTES", 4<<30),
		RatePerSecond:             integer("ICINGA_KUBERNETES_RATE_PER_SECOND", 200),
		RateBurst:                 integer("ICINGA_KUBERNETES_RATE_BURST", 400),
	}

	var err error
	c.FederationTargets, err = parseFederationTargets(os.Getenv("ICINGA_KUBERNETES_FEDERATION_TARGETS"))
	if err != nil {
		return Config{}, err
	}
	for _, target := range c.FederationTargets {
		if strings.EqualFold(target.Name, c.ClusterName) {
			return Config{}, fmt.Errorf("federation target %q must not reference the local cluster", target.Name)
		}
	}

	switch c.Role {
	case "api", "worker", "collector", "migrate":
	default:
		return Config{}, fmt.Errorf("unsupported role %q", c.Role)
	}
	if c.ClusterName == "" {
		return Config{}, fmt.Errorf("ICINGA_KUBERNETES_CLUSTER_NAME is required")
	}
	if c.Role == "collector" {
		if err := validateHTTPBaseURL(c.APIURL); err != nil {
			return Config{}, fmt.Errorf("ICINGA_KUBERNETES_API_URL: %w", err)
		}
	}
	if c.MetricsURL != "" {
		if err := validateHTTPBaseURL(c.MetricsURL); err != nil {
			return Config{}, fmt.Errorf("ICINGA_KUBERNETES_METRICS_URL: %w", err)
		}
	}
	if c.NotificationsURL != "" {
		if c.NotificationsWebURL == "" {
			return Config{}, fmt.Errorf("ICINGA_KUBERNETES_NOTIFICATIONS_WEB_URL is required when notifications are enabled")
		}
		if err := validateHTTPBaseURL(c.NotificationsWebURL); err != nil {
			return Config{}, fmt.Errorf("ICINGA_KUBERNETES_NOTIFICATIONS_WEB_URL: %w", err)
		}
		if c.Role != "worker" {
			return Config{}, fmt.Errorf("ICINGA_KUBERNETES_NOTIFICATIONS_URL is only valid for the worker role")
		}
		if err := validateHTTPBaseURL(c.NotificationsURL); err != nil {
			return Config{}, fmt.Errorf("ICINGA_KUBERNETES_NOTIFICATIONS_URL: %w", err)
		}
		if c.NotificationsUsername == "" {
			return Config{}, fmt.Errorf("ICINGA_KUBERNETES_NOTIFICATIONS_USERNAME is required when notifications are enabled")
		}
		if (c.NotificationsPassword == "") == (c.NotificationsPasswordFile == "") {
			return Config{}, fmt.Errorf("exactly one of ICINGA_KUBERNETES_NOTIFICATIONS_PASSWORD and ICINGA_KUBERNETES_NOTIFICATIONS_PASSWORD_FILE is required")
		}
	} else if c.NotificationsUsername != "" || c.NotificationsPassword != "" || c.NotificationsPasswordFile != "" || c.NotificationsCAFile != "" {
		return Config{}, fmt.Errorf("ICINGA_KUBERNETES_NOTIFICATIONS_URL is required when notifications options are set")
	}
	if c.Role != "collector" && c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("ICINGA_KUBERNETES_DATABASE_URL is required for role %s", c.Role)
	}
	if c.Role == "api" {
		credentials := []struct {
			role  string
			value string
			path  string
		}{
			{role: "collector", value: c.CollectorToken, path: c.CollectorTokenFile},
			{role: "reader", value: c.ReaderToken, path: c.ReaderTokenFile},
			{role: "federation", value: c.FederationToken, path: c.FederationTokenFile},
			{role: "admin", value: c.AdminToken, path: c.AdminTokenFile},
		}
		for _, credential := range credentials {
			if err := validateCredential(credential.value, credential.path); err != nil {
				return Config{}, fmt.Errorf("%s credential for API role: %w", credential.role, err)
			}
		}
	}
	if c.Role == "collector" {
		if err := validateCredential(c.CollectorToken, c.CollectorTokenFile); err != nil {
			return Config{}, fmt.Errorf("collector credential for collector role: %w", err)
		}
	}
	if c.BatchSize > 1000 {
		return Config{}, fmt.Errorf("ICINGA_KUBERNETES_BATCH_SIZE must not exceed 1000")
	}
	if c.StaleAfter >= c.UnavailableAfter {
		return Config{}, fmt.Errorf("ICINGA_KUBERNETES_STALE_AFTER must be shorter than ICINGA_KUBERNETES_UNAVAILABLE_AFTER")
	}
	return c, nil
}

func validateHTTPBaseURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("must be an absolute HTTP(S) URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("must not contain credentials, query or fragment")
	}
	return nil
}

func validateCredential(value, path string) error {
	if value != "" {
		return nil
	}
	if path == "" {
		return fmt.Errorf("is required")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read token file: %w", err)
	}
	if strings.TrimSpace(string(content)) == "" {
		return fmt.Errorf("token file is empty")
	}
	return nil
}

func parseFederationTargets(rawTargets string) ([]FederationTarget, error) {
	if strings.TrimSpace(rawTargets) == "" {
		return nil, nil
	}
	targets := make([]FederationTarget, 0)
	names := map[string]bool{}
	for _, raw := range strings.Split(rawTargets, ",") {
		if len(targets) >= 64 {
			return nil, fmt.Errorf("at most 64 federation targets are supported")
		}
		parts := strings.SplitN(strings.TrimSpace(raw), "=", 2)
		if len(parts) != 2 || len(parts[0]) > 253 || !federationNamePattern.MatchString(parts[0]) || parts[1] == "" || len(parts[1]) > 2048 {
			return nil, fmt.Errorf("invalid ICINGA_KUBERNETES_FEDERATION_TARGETS entry %q; expected name=url", raw)
		}
		nameKey := strings.ToLower(parts[0])
		if names[nameKey] {
			return nil, fmt.Errorf("duplicate federation target %q", parts[0])
		}
		parsed, err := url.Parse(parts[1])
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, fmt.Errorf("federation target %q must be an HTTP(S) base URL without credentials, query or fragment", parts[0])
		}
		names[nameKey] = true
		targets = append(targets, FederationTarget{Name: parts[0], URL: strings.TrimRight(parts[1], "/")})
	}
	return targets, nil
}

func validateExplicitEnvironment() error {
	for _, key := range []string{
		"ICINGA_KUBERNETES_EVENT_RETENTION", "ICINGA_KUBERNETES_TOMBSTONE_RETENTION", "ICINGA_KUBERNETES_HISTORY_RETENTION",
		"ICINGA_KUBERNETES_STALE_AFTER", "ICINGA_KUBERNETES_UNAVAILABLE_AFTER",
	} {
		if raw := os.Getenv(key); raw != "" {
			parsed, err := time.ParseDuration(raw)
			if err != nil || parsed <= 0 {
				return fmt.Errorf("%s must be a positive Go duration", key)
			}
		}
	}
	for _, key := range []string{"ICINGA_KUBERNETES_BATCH_SIZE", "ICINGA_KUBERNETES_RATE_PER_SECOND", "ICINGA_KUBERNETES_RATE_BURST"} {
		if raw := os.Getenv(key); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 {
				return fmt.Errorf("%s must be a positive integer", key)
			}
		}
	}
	if raw := os.Getenv("ICINGA_KUBERNETES_SPOOL_MAX_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 1 {
			return fmt.Errorf("ICINGA_KUBERNETES_SPOOL_MAX_BYTES must be a positive integer")
		}
	}
	if raw := os.Getenv("ICINGA_KUBERNETES_METRICS_INSECURE"); raw != "" {
		if _, err := strconv.ParseBool(raw); err != nil {
			return fmt.Errorf("ICINGA_KUBERNETES_METRICS_INSECURE must be a boolean")
		}
	}
	return nil
}

func value(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func duration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func integer(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil || i < 1 {
		return fallback
	}
	return i
}

func integer64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil || i < 1 {
		return fallback
	}
	return i
}

func boolean(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
