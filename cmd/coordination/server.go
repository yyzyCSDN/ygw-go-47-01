package main

import (
	"net/http"
	"time"

	coordination "coordination"
	"coordination/internal/election"
	"coordination/internal/lease"
	"coordination/internal/lock"
	"coordination/internal/session"
	"coordination/internal/store"
	"coordination/internal/watch"
)

// Server wires every component behind the HTTP API.
type Server struct {
	cfg      Config
	store    *store.Store
	leases   *lease.Manager
	locks    *lock.Manager
	watcher  *watch.Watcher
	election *election.Election
	sessions *session.Manager
	clock    lease.Clock
}

// NewServer builds a server from the wiring root.
func NewServer(cfg Config, st *store.Store, leases *lease.Manager, locks *lock.Manager, watcher *watch.Watcher, el *election.Election, sessions *session.Manager, clock lease.Clock) *Server {
	return &Server{
		cfg:      cfg,
		store:    st,
		leases:   leases,
		locks:    locks,
		watcher:  watcher,
		election: el,
		sessions: sessions,
		clock:    clock,
	}
}

// Routes returns the HTTP handler tree.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/", s.handleConsole)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/kv/put", s.handleKVPut)
	mux.HandleFunc("/api/kv/snapshot", s.handleKVSnapshot)
	mux.HandleFunc("/api/kv/", s.handleKVGet)
	mux.HandleFunc("/api/lease/create", s.handleLeaseCreate)
	mux.HandleFunc("/api/lease/renew", s.handleLeaseRenew)
	mux.HandleFunc("/api/lease/batch-renew", s.handleLeaseBatchRenew)
	mux.HandleFunc("/api/lease/revoke", s.handleLeaseRevoke)
	mux.HandleFunc("/api/lease/", s.handleLeaseGet)
	mux.HandleFunc("/api/lock/acquire", s.handleLockAcquire)
	mux.HandleFunc("/api/lock/release", s.handleLockRelease)
	mux.HandleFunc("/api/lock/snapshot", s.handleLockSnapshot)
	mux.HandleFunc("/api/watch", s.handleWatch)
	mux.HandleFunc("/api/session/connect", s.handleSessionConnect)
	mux.HandleFunc("/api/session/reconnect", s.handleSessionReconnect)
	mux.HandleFunc("/api/session/status", s.handleSessionStatus)
	mux.HandleFunc("/api/session/resume-all", s.handleSessionResumeAll)
	mux.HandleFunc("/api/election/campaign", s.handleElectionCampaign)
	mux.HandleFunc("/api/election/handoff", s.handleElectionHandoff)
	mux.HandleFunc("/api/election/resign", s.handleElectionResign)
	mux.HandleFunc("/api/election/status", s.handleElectionStatus)
	return mux
}

func nowOf(s *Server) time.Time {
	return s.clock.Now()
}

func (s *Server) handleConsole(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	coordination.ServeConsole(w, r)
}
