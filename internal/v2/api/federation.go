package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
)

const (
	federationTimeout    = 250 * time.Millisecond
	federationCacheTTL   = 5 * time.Second
	federationRetryDelay = 5 * time.Second
	federationMaxPending = 32
)

type federationResponse struct {
	status    int
	body      []byte
	freshness string
	fetched   time.Time
}

type federationCall struct {
	done     chan struct{}
	response federationResponse
	err      error
}

type federatedResult struct {
	body      []byte
	freshness string
	err       error
}

func (s *Server) federatedResults(ctx context.Context, path string, body []byte) []federatedResult {
	results := make([]federatedResult, len(s.Config.FederationTargets))
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].body, results[i].freshness, results[i].err = s.callFederated(ctx, &s.Config.FederationTargets[i], http.MethodPost, path, body)
		}()
	}
	wg.Wait()
	return results
}

// Cached reads never wait for a remote server. Refreshes are coalesced and
// bounded; a cold miss has a short deadline and a failed target gets a cooldown.
func (s *Server) federationRead(ctx context.Context, target *config.FederationTarget, method, path string, body []byte) (federationResponse, error) {
	sum := sha256.Sum256(append([]byte(target.URL+"\x00"+method+"\x00"+path+"\x00"), body...))
	key := fmt.Sprintf("%x", sum[:])
	cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	status, cached, source, fetched, cacheErr := s.Store.GetFederationCache(cacheCtx, target.Name, key)
	cancel()
	haveCache := cacheErr == nil && validFreshness(source) && json.Valid(cached)
	response := federationResponse{status: status, body: cached, freshness: source, fetched: fetched}
	if haveCache {
		response.freshness = worseFreshness(source, federationFreshness(time.Since(fetched), s.Config.StaleAfter, s.Config.UnavailableAfter))
		if time.Since(fetched) < federationCacheTTL {
			return response, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return federationResponse{}, err
	}
	targetKey := target.Name + "\x00" + target.URL
	flightKey := targetKey + "\x00" + key
	s.federationMu.Lock()
	if s.federationPending == nil {
		s.federationPending = make(map[string]*federationCall)
		s.federationRetry = make(map[string]time.Time)
		s.federationProbe = make(map[string]bool)
	}
	call := s.federationPending[flightKey]
	knownFailed := !s.federationRetry[targetKey].IsZero()
	if call == nil && !s.federationProbe[targetKey] && time.Now().After(s.federationRetry[targetKey]) && len(s.federationPending) < federationMaxPending {
		call = &federationCall{done: make(chan struct{})}
		s.federationPending[flightKey] = call
		if knownFailed {
			// One recovery probe per failed target, across all paths/cache keys.
			s.federationProbe[targetKey] = true
		}
		targetCopy := *target
		bodyCopy := bytes.Clone(body)
		go func() {
			refreshCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), federationTimeout)
			defer stop()
			call.response, call.err = s.fetchFederation(refreshCtx, &targetCopy, method, path, bodyCopy)
			if call.err == nil && call.response.status >= 200 && call.response.status < 300 && json.Valid(call.response.body) && !federationLogs(path) {
				_ = s.Store.PutFederationCache(refreshCtx, targetCopy.Name, key, call.response.status, call.response.body, call.response.freshness)
			}
			s.federationMu.Lock()
			if call.err != nil {
				s.federationRetry[targetKey] = time.Now().Add(federationRetryDelay)
			} else {
				delete(s.federationRetry, targetKey)
			}
			if knownFailed {
				delete(s.federationProbe, targetKey)
			}
			delete(s.federationPending, flightKey)
			close(call.done)
			s.federationMu.Unlock()
		}()
	}
	s.federationMu.Unlock()
	if haveCache {
		return response, nil
	}
	if knownFailed {
		// Recovery probes run in the background; users never wait on a target
		// already known to be unavailable, even after the cooldown expires.
		return federationResponse{}, errors.New("federation target unavailable; recovery check pending")
	}
	if call == nil {
		return federationResponse{}, errors.New("federation target cooling down or busy")
	}
	select {
	case <-ctx.Done():
		return federationResponse{}, ctx.Err()
	case <-call.done:
		return call.response, call.err
	}
}

func (s *Server) fetchFederation(ctx context.Context, target *config.FederationTarget, method, path string, body []byte) (federationResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(target.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return federationResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token("federation"))
	req.Header.Set("Content-Type", "application/json")
	if method == http.MethodPost && strings.HasPrefix(path, "/api/v1/live/metrics/prometheus/") {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	req.Header.Set("X-Icinga-Federation-Hop", s.Config.ClusterName)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return federationResponse{}, err
	}
	defer resp.Body.Close()
	data, err := readBounded(resp.Body, 16<<20)
	if err != nil {
		return federationResponse{}, err
	}
	if resp.StatusCode >= 500 {
		return federationResponse{}, fmt.Errorf("remote returned %s", resp.Status)
	}
	freshness := resp.Header.Get("X-Icinga-Freshness")
	if freshness == "" {
		freshness = "live"
	}
	if !validFreshness(freshness) || (!federationLogs(path) && !json.Valid(data)) {
		return federationResponse{}, errors.New("invalid federation response")
	}
	return federationResponse{status: resp.StatusCode, body: data, freshness: freshness, fetched: time.Now()}, nil
}

func federationLogs(path string) bool {
	return strings.HasPrefix(path, "/api/v1/live/pods/") && strings.HasSuffix(strings.SplitN(path, "?", 2)[0], "/logs")
}
