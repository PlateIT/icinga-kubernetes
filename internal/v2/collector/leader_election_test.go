package collector

import (
	"context"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestLeaderElectionHandsLeaseToAnotherReplica(t *testing.T) {
	client := fake.NewSimpleClientset()
	const (
		namespace = "monitoring"
		name      = "icinga-kubernetes-pods"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()

	started := make(chan string, 2)
	finished := make(chan string, 2)
	errors := make(chan error, 2)
	var wg sync.WaitGroup
	start := func(electionCtx context.Context, identity string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock := &resourcelock.LeaseLock{
				LeaseMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Client:    client.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity: identity,
				},
			}
			err := runLeaderElection(electionCtx, leaderelection.LeaderElectionConfig{
				Lock:            lock,
				LeaseDuration:   2 * time.Second,
				RenewDeadline:   time.Second,
				RetryPeriod:     200 * time.Millisecond,
				ReleaseOnCancel: true,
				Name:            name,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(leaderCtx context.Context) {
						started <- identity
						<-leaderCtx.Done()
						finished <- identity
					},
					OnStoppedLeading: func() {},
				},
			})
			errors <- err
		}()
	}

	start(firstCtx, "collector-1")
	if leader := receiveBefore(t, ctx, started); leader != "collector-1" {
		t.Fatalf("first leader = %q", leader)
	}

	start(secondCtx, "collector-2")
	stopFirst()
	if replica := receiveBefore(t, ctx, finished); replica != "collector-1" {
		t.Fatalf("released replica = %q", replica)
	}
	if leader := receiveBefore(t, ctx, started); leader != "collector-2" {
		t.Fatalf("successor leader = %q", leader)
	}

	lease, err := client.CoordinationV1().Leases(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "collector-2" {
		t.Fatalf("lease holder = %v", lease.Spec.HolderIdentity)
	}

	stopSecond()
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("leader election: %v", err)
		}
	}
}

func receiveBefore(t *testing.T, ctx context.Context, values <-chan string) string {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-ctx.Done():
		t.Fatal("timed out waiting for leader-election transition")
		return ""
	}
}
