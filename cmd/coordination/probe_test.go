package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coordination "coordination"
	"coordination/internal/election"
	"coordination/internal/lease"
	"coordination/internal/lock"
	"coordination/internal/model"
	"coordination/internal/session"
	"coordination/internal/store"
	"coordination/internal/watch"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := loadConfig()
	clock := lease.WallClock{}
	st := store.New(64)
	leases := lease.New(clock, nil)
	locks := lock.New(leases, st)
	watcher := watch.New(st)
	el := election.New("test-node", leases, time.Minute, time.Second, clock)
	el.Follow()
	sessions := session.New(context.Background(), leases, watcher, st, clock, time.Minute)
	leases.OnExpire(func(id model.LeaseID, now time.Time) {
		locks.ReleaseByLease(id, now)
	})
	server := NewServer(cfg, st, leases, locks, watcher, el, sessions, clock)
	ts := httptest.NewServer(server.Routes())
	t.Cleanup(ts.Close)
	return ts
}

func TestHealthz(t *testing.T) {
	ts := newTestServer(t)
	response, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", response.StatusCode)
	}
}

func TestConsoleMarker(t *testing.T) {
	ts := newTestServer(t)
	response, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(body), coordination.ConsoleMarker) {
		t.Fatal("console marker missing")
	}
}

func TestKVThroughAPI(t *testing.T) {
	ts := newTestServer(t)
	payload := []byte(`{"key":"alpha","value":"one"}`)
	response, err := http.Post(ts.URL+"/api/kv/put", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("kv put = %d", response.StatusCode)
	}
	response, err = http.Get(ts.URL + "/api/kv/alpha")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result map[string]any
	_ = json.NewDecoder(response.Body).Decode(&result)
	if result["value"] != "one" {
		t.Fatalf("kv get = %v", result)
	}
}

func TestLockThroughAPI(t *testing.T) {
	ts := newTestServer(t)
	payload := []byte(`{"id":"job-lock","client":"worker-a"}`)
	response, err := http.Post(ts.URL+"/api/lock/acquire", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("lock acquire = %d", response.StatusCode)
	}
	response, err = http.Post(ts.URL+"/api/lock/release", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("lock release = %d", response.StatusCode)
	}
}

func TestStatusReportsCounts(t *testing.T) {
	ts := newTestServer(t)
	response, err := http.Get(ts.URL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result map[string]any
	_ = json.NewDecoder(response.Body).Decode(&result)
	if result["keys"] == nil || result["leases"] == nil {
		t.Fatalf("status missing fields: %v", fmt.Sprint(result))
	}
}
