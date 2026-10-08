// Package kubeauth provides short-lived credentials for explicitly selected
// service accounts. The pod identity remains responsible for TokenRequests.
package kubeauth

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

type Source struct {
	mu       sync.Mutex
	accounts corev1.ServiceAccountInterface
	name     string
	token    string
	expires  time.Time
	refresh  time.Time
	now      func() time.Time
}

func New(accounts corev1.ServiceAccountInterface, name string) *Source {
	return &Source{accounts: accounts, name: name, now: time.Now}
}

func (s *Source) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.token != "" && now.Before(s.refresh) && now.Before(s.expires) {
		return s.token, nil
	}
	ttl := int64(600)
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r, err := s.accounts.CreateToken(requestCtx, s.name, &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &ttl},
	}, metav1.CreateOptions{})
	now = s.now() // Account for time spent waiting for the API server.
	if err == nil && (r.Status.Token == "" || !r.Status.ExpirationTimestamp.Time.After(now)) {
		err = fmt.Errorf("empty or expired TokenRequest response")
	}
	if err != nil {
		// A transient refresh failure must not discard a still-valid credential.
		// Retry soon, but never extend the API server's actual expiration time.
		s.refresh = now.Add(15 * time.Second)
		if s.token != "" && now.Before(s.expires) {
			return s.token, nil
		}
		return "", fmt.Errorf("request token for service account %s: %w", s.name, err)
	}
	s.token = r.Status.Token
	s.expires = r.Status.ExpirationTimestamp.Time
	s.refresh = now.Add(s.expires.Sub(now) * 4 / 5)
	return s.token, nil
}

func (s *Source) Invalidate(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token == s.token {
		s.token = ""
	}
}

type transport struct {
	base   http.RoundTripper
	source *Source
}

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) {
	token, err := t.source.Token(r.Context())
	if err != nil {
		return nil, err
	}
	request := r.Clone(r.Context())
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := t.base.RoundTrip(request)
	if response != nil && response.StatusCode == http.StatusUnauthorized {
		t.source.Invalidate(token)
	}
	return response, err
}

// Config retains the API endpoint and CA, but never mixes authentication
// mechanisms or falls back to the pod's identity for a delegated request.
func Config(base *rest.Config, source *Source) *rest.Config {
	c := rest.CopyConfig(base)
	c.BearerToken, c.BearerTokenFile, c.Username, c.Password = "", "", "", ""
	c.CertFile, c.KeyFile, c.CertData, c.KeyData = "", "", nil, nil
	c.AuthProvider, c.ExecProvider = nil, nil
	c.Impersonate = rest.ImpersonationConfig{}
	c.WrapTransport = func(base http.RoundTripper) http.RoundTripper { return transport{base, source} }
	return c
}
