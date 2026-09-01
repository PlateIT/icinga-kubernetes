package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestCollectorLeaseProcessHelper(t *testing.T) {
	if os.Getenv("ICINGA_KUBERNETES_COLLECTOR_LEASE_HELPER") != "1" {
		return
	}
	host := os.Getenv("ICINGA_KUBERNETES_TEST_LEASE_API")
	identity := os.Getenv("ICINGA_KUBERNETES_TEST_LEASE_IDENTITY")
	config := &rest.Config{Host: host, ContentConfig: rest.ContentConfig{
		AcceptContentTypes: runtime.ContentTypeJSON,
		ContentType:        runtime.ContentTypeJSON,
	}}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: "process-failover", Namespace: "icinga"},
		Client:    client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}
	err = runLeaderElection(context.Background(), leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   2 * time.Second,
		RenewDeadline:   1500 * time.Millisecond,
		RetryPeriod:     200 * time.Millisecond,
		ReleaseOnCancel: false,
		Name:            "collector-process-failover",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) { <-ctx.Done() },
			OnStoppedLeading: func() {},
			OnNewLeader:      func(string) {},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

type leaseAPIServer struct {
	mu    sync.Mutex
	lease *coordinationv1.Lease
	rv    int64
}

func (s *leaseAPIServer) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	const collection = "/apis/coordination.k8s.io/v1/namespaces/icinga/leases"
	const resourcePath = collection + "/process-failover"
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case request.Method == http.MethodGet && request.URL.Path == resourcePath:
		if s.lease == nil {
			writeKubernetesStatus(response, http.StatusNotFound, "NotFound")
			return
		}
		_ = json.NewEncoder(response).Encode(s.lease)
	case request.Method == http.MethodPost && request.URL.Path == collection:
		if s.lease != nil {
			writeKubernetesStatus(response, http.StatusConflict, "AlreadyExists")
			return
		}
		lease, err := decodeLease(request.Body)
		if err != nil {
			writeKubernetesStatus(response, http.StatusBadRequest, err.Error())
			return
		}
		s.rv = 1
		lease.ResourceVersion = fmt.Sprint(s.rv)
		lease.TypeMeta = metav1.TypeMeta{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}
		s.lease = lease
		response.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(response).Encode(s.lease)
	case request.Method == http.MethodPut && request.URL.Path == resourcePath:
		if s.lease == nil {
			writeKubernetesStatus(response, http.StatusNotFound, "NotFound")
			return
		}
		lease, err := decodeLease(request.Body)
		if err != nil {
			writeKubernetesStatus(response, http.StatusBadRequest, err.Error())
			return
		}
		if lease.ResourceVersion != s.lease.ResourceVersion {
			writeKubernetesStatus(response, http.StatusConflict, "Conflict")
			return
		}
		s.rv++
		lease.ResourceVersion = fmt.Sprint(s.rv)
		lease.TypeMeta = metav1.TypeMeta{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}
		s.lease = lease
		_ = json.NewEncoder(response).Encode(s.lease)
	default:
		writeKubernetesStatus(response, http.StatusNotFound, "NotFound")
	}
}

func (s *leaseAPIServer) holder() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil || s.lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *s.lease.Spec.HolderIdentity
}

func decodeLease(body io.Reader) (*coordinationv1.Lease, error) {
	var lease coordinationv1.Lease
	if err := json.NewDecoder(io.LimitReader(body, 1<<20)).Decode(&lease); err != nil {
		return nil, err
	}
	return &lease, nil
}

func writeKubernetesStatus(response http.ResponseWriter, code int, reason string) {
	response.WriteHeader(code)
	_ = json.NewEncoder(response).Encode(metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusFailure,
		Reason:   metav1.StatusReason(reason),
		Code:     int32(code),
	})
}

type collectorChild struct {
	cmd     *exec.Cmd
	output  *synchronizedBuffer
	stopped bool
}

type synchronizedBuffer struct {
	mu      sync.Mutex
	content strings.Builder
}

func (b *synchronizedBuffer) Write(content []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.content.Write(content)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.content.String()
}

func TestCollectorLeaseTransfersAfterProcessFailure(t *testing.T) {
	leaseAPI := &leaseAPIServer{}
	server := httptest.NewServer(leaseAPI)
	defer server.Close()
	children := make([]*collectorChild, 0, 2)
	defer func() {
		for _, child := range children {
			if !child.stopped {
				_ = child.cmd.Process.Kill()
				_ = child.cmd.Wait()
				child.stopped = true
			}
		}
	}()
	start := func(identity string) *collectorChild {
		t.Helper()
		output := &synchronizedBuffer{}
		cmd := exec.Command(os.Args[0], "-test.run=^TestCollectorLeaseProcessHelper$")
		cmd.Env = collectorHelperEnvironment(server.URL, identity)
		cmd.Stdout = output
		cmd.Stderr = output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		child := &collectorChild{cmd: cmd, output: output}
		children = append(children, child)
		return child
	}

	first := start("collector-1")
	waitForLeaseHolder(t, leaseAPI, "collector-1", 5*time.Second, first)
	second := start("collector-2")
	assertLeaseHolderFor(t, leaseAPI, "collector-1", 500*time.Millisecond)
	if err := first.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := first.cmd.Wait(); err == nil {
		t.Fatal("killed collector process exited successfully")
	}
	first.stopped = true
	waitForLeaseHolder(t, leaseAPI, "collector-2", 6*time.Second, second)
}

func collectorHelperEnvironment(api, identity string) []string {
	overrides := map[string]string{
		"ICINGA_KUBERNETES_COLLECTOR_LEASE_HELPER": "1",
		"ICINGA_KUBERNETES_TEST_LEASE_API":         api,
		"ICINGA_KUBERNETES_TEST_LEASE_IDENTITY":    identity,
	}
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		remove := false
		for key := range overrides {
			if strings.EqualFold(name, key) {
				remove = true
				break
			}
		}
		if !remove {
			environment = append(environment, entry)
		}
	}
	for key, value := range overrides {
		environment = append(environment, key+"="+value)
	}
	return environment
}

func assertLeaseHolderFor(t *testing.T, api *leaseAPIServer, expected string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if holder := api.holder(); holder != expected {
			t.Fatalf("live leader changed unexpectedly from %q to %q", expected, holder)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForLeaseHolder(t *testing.T, api *leaseAPIServer, expected string, timeout time.Duration, child *collectorChild) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if holder := api.holder(); holder == expected {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("lease holder did not become %q (current %q)\n%s", expected, api.holder(), child.output.String())
}
