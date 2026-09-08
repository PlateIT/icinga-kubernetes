package live

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type Gateway struct {
	cfg        config.Config
	dynamic    dynamic.Interface
	discovery  discovery.DiscoveryInterface
	kubernetes kubernetes.Interface
	http       *http.Client
}

func New(cfg config.Config) (*Gateway, error) {
	if cfg.MetricsURL != "" && cfg.MetricsTokenFile != "" {
		token, err := os.ReadFile(cfg.MetricsTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read metrics token: %w", err)
		}
		if strings.TrimSpace(string(token)) == "" {
			return nil, fmt.Errorf("metrics token file is empty")
		}
	}
	kcfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}
	d, err := dynamic.NewForConfig(kcfg)
	if err != nil {
		return nil, err
	}
	disc, err := discovery.NewDiscoveryClientForConfig(kcfg)
	if err != nil {
		return nil, err
	}
	k, err := kubernetes.NewForConfig(kcfg)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.MetricsInsecure}
	if cfg.MetricsCAFile != "" {
		ca, err := os.ReadFile(cfg.MetricsCAFile)
		if err != nil {
			return nil, fmt.Errorf("read metrics CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("metrics CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	transport.TLSClientConfig = tlsConfig
	return &Gateway{cfg: cfg, dynamic: d, discovery: disc, kubernetes: k, http: &http.Client{Timeout: 30 * time.Second, Transport: transport}}, nil
}

func (g *Gateway) Manifest(ctx context.Context, r model.Resource) (map[string]any, error) {
	gv := r.Version
	if r.Group != "" && r.Group != "core" {
		gv = r.Group + "/" + r.Version
	}
	resources, err := g.discovery.ServerResourcesForGroupVersion(gv)
	if err != nil {
		return nil, err
	}
	resourceName := ""
	namespaced := false
	for _, candidate := range resources.APIResources {
		if candidate.Kind == r.Kind && !strings.Contains(candidate.Name, "/") {
			resourceName = candidate.Name
			namespaced = candidate.Namespaced
			break
		}
	}
	if resourceName == "" {
		return nil, fmt.Errorf("resource kind %s is no longer served", r.Kind)
	}
	client := g.dynamic.Resource(schema.GroupVersionResource{Group: mapGroup(r.Group), Version: r.Version, Resource: resourceName})
	var obj map[string]any
	if namespaced {
		item, err := client.Namespace(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		obj = item.Object
	} else {
		item, err := client.Get(ctx, r.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		obj = item.Object
	}
	if strings.EqualFold(r.Kind, "Secret") {
		delete(obj, "data")
		delete(obj, "stringData")
	}
	return obj, nil
}

func (g *Gateway) Logs(ctx context.Context, namespace, pod, container string, tail int64) (io.ReadCloser, error) {
	if tail < 1 || tail > 10000 {
		tail = 500
	}
	return g.kubernetes.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{Container: container, TailLines: &tail, Timestamps: true}).Stream(ctx)
}

func (g *Gateway) Metrics(ctx context.Context, query string, at *time.Time) (json.RawMessage, error) {
	if g.cfg.MetricsURL == "" {
		return nil, fmt.Errorf("metrics endpoint is not configured")
	}
	values := url.Values{"query": []string{query}}
	if at != nil {
		values.Set("time", strconv.FormatInt(at.Unix(), 10))
	}
	return g.metricsRequest(ctx, "/api/v1/query?"+values.Encode())
}

func (g *Gateway) MetricsRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (json.RawMessage, error) {
	if g.cfg.MetricsURL == "" {
		return nil, fmt.Errorf("metrics endpoint is not configured")
	}
	values := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
		"step":  {strconv.FormatInt(int64(step.Seconds()), 10)},
	}
	return g.metricsRequest(ctx, "/api/v1/query_range?"+values.Encode())
}

func (g *Gateway) metricsRequest(ctx context.Context, path string) (json.RawMessage, error) {
	return g.MetricsProxy(ctx, http.MethodGet, path, nil, "")
}

// MetricsProxy forwards a bounded, read-only Prometheus HTTP API request to
// the metrics endpoint of this cluster. Callers must validate the endpoint and
// query cost before invoking it.
func (g *Gateway) MetricsProxy(ctx context.Context, method, path string, requestBody io.Reader, contentType string) (json.RawMessage, error) {
	if g.cfg.MetricsURL == "" {
		return nil, fmt.Errorf("metrics endpoint is not configured")
	}
	req, err := http.NewRequestWithContext(ctx, method, g.cfg.MetricsURL+path, requestBody)
	if err != nil {
		return nil, err
	}
	if g.cfg.MetricsNamespace != "" {
		query := req.URL.Query()
		query.Set("namespace", g.cfg.MetricsNamespace)
		req.URL.RawQuery = query.Encode()
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if g.cfg.MetricsTokenFile != "" {
		token, err := os.ReadFile(g.cfg.MetricsTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read metrics token: %w", err)
		}
		value := strings.TrimSpace(string(token))
		if value == "" {
			return nil, fmt.Errorf("metrics token file is empty")
		}
		req.Header.Set("Authorization", "Bearer "+value)
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(responseBody) > 8<<20 {
		return nil, fmt.Errorf("metrics endpoint response exceeds 8388608 bytes")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("metrics endpoint returned %s", resp.Status)
	}
	if !json.Valid(responseBody) {
		return nil, fmt.Errorf("metrics endpoint returned invalid JSON")
	}
	return responseBody, nil
}

func mapGroup(group string) string {
	if group == "core" {
		return ""
	}
	return group
}
