package kubeauth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

func TestRefreshExpirationAndRevocation(t *testing.T) {
	now := time.Now()
	client := fake.NewSimpleClientset()
	calls, unavailable := 0, false
	client.PrependReactor("create", "serviceaccounts", func(a ktesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "token" {
			t.Fatalf("unexpected action: %v", a)
		}
		calls++
		if unavailable {
			return true, nil, fmt.Errorf("API unavailable")
		}
		return true, &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{
			Token: fmt.Sprintf("token-%d", calls), ExpirationTimestamp: metav1.NewTime(now.Add(600 * time.Second)),
		}}, nil
	})
	s := New(client.CoreV1().ServiceAccounts("monitoring"), "cluster-reader")
	s.now = func() time.Time { return now }
	first, err := s.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Token(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("cached token triggered %d requests", calls)
	}
	now = now.Add(481 * time.Second)
	unavailable = true
	if token, err := s.Token(context.Background()); err != nil || token != first {
		t.Fatalf("valid token lost during refresh failure: %v", err)
	}
	now = now.Add(120 * time.Second)
	if _, err := s.Token(context.Background()); err == nil {
		t.Fatal("expired token reused")
	}
	unavailable = false
	second, err := s.Token(context.Background())
	if err != nil || second == first {
		t.Fatalf("renewal failed: %v", err)
	}
	s.Invalidate(first)
	if token, _ := s.Token(context.Background()); token != second {
		t.Fatal("old 401 invalidated newer token")
	}
	s.Invalidate(second)
	if token, _ := s.Token(context.Background()); token == second {
		t.Fatal("revoked token reused")
	}
}

func TestDelegatedTransportNeverUsesBaseCredential(t *testing.T) {
	client := fake.NewSimpleClientset()
	calls := 0
	client.PrependReactor("create", "serviceaccounts", func(a ktesting.Action) (bool, runtime.Object, error) {
		calls++
		return true, &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{Token: fmt.Sprintf("delegated-%d", calls), ExpirationTimestamp: metav1.NewTime(time.Now().Add(time.Minute))}}, nil
	})
	s := New(client.CoreV1().ServiceAccounts("monitoring"), "cluster-reader")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != fmt.Sprintf("Bearer delegated-%d", calls) {
			t.Errorf("wrong identity: %s", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer upstream.Close()
	cfg := Config(&rest.Config{Host: upstream.URL, BearerToken: "namespace-token", Username: "old", Password: "old"}, s)
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, upstream.URL, nil)
	for i := 0; i < 2; i++ {
		response, err := transport.RoundTrip(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	}
	if calls != 2 {
		t.Fatalf("401 did not refresh identity: %d", calls)
	}
	if request.Header.Get("Authorization") != "" {
		t.Fatal("mutated caller request")
	}
}
