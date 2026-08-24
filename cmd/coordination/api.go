package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"coordination/internal/election"
	"coordination/internal/lease"
	"coordination/internal/model"
	"coordination/internal/store"
	"coordination/internal/watch"
)

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func readJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	return decoder.Decode(target)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	role, term, leader := s.election.Current()
	snap := s.store.Snapshot(s.store.CurrentRev())
	shards := store.ShardCount(s.store)
	connected := len(s.sessions.SessionsByState(model.SessionConnected))
	disconnected := len(s.sessions.SessionsByState(model.SessionDisconnected))
	topicSamples := make(map[string]uint32, 8)
	for key := range snap {
		if len(topicSamples) >= 8 {
			break
		}
		topicSamples[key] = watch.Topic(key, shards)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node":          s.cfg.NodeName,
		"clock_mode":    clockMode(s.clock),
		"current_rev":   s.store.CurrentRev(),
		"keys":          len(snap),
		"shards":        shards,
		"leases":        s.leases.Count(),
		"locks":         s.locks.Count(),
		"watches":       s.watcher.Count(),
		"sessions":      s.sessions.Count(),
		"connected":     connected,
		"disconnected":  disconnected,
		"election_role": role.String(),
		"term":          term,
		"leader":        leader,
		"can_lead":      model.CanLead(role),
		"leader_lease":  s.election.LeaderLease(),
		"topic_samples": topicSamples,
	})
}

func clockMode(c lease.Clock) string {
	if _, ok := c.(lease.WallClock); ok {
		return "wall"
	}
	return "manual"
}

type kvPutRequest struct {
	Key     string        `json:"key"`
	Value   string        `json:"value"`
	LeaseID model.LeaseID `json:"lease_id,omitempty"`
}

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request) {
	var req kvPutRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if req.Key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "key is required"})
		return
	}
	if req.LeaseID != "" {
		if err := s.leases.BindKey(req.LeaseID, req.Key); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
	}
	rev, err := s.store.Put(req.Key, []byte(req.Value))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rev": rev})
}

func (s *Server) handleKVSnapshot(w http.ResponseWriter, r *http.Request) {
	rev := s.store.CurrentRev()
	if raw := r.URL.Query().Get("rev"); raw != "" {
		if parsed, err := strconv.ParseUint(raw, 10, 64); err == nil {
			rev = model.Revision(parsed)
		}
	}
	snap := s.store.Snapshot(rev)
	writeJSON(w, http.StatusOK, map[string]any{"rev": rev, "entries": snap})
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/api/kv/")
	if key == "" || strings.Contains(key, "/") {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "key not found"})
		return
	}
	reader, err := s.store.BeginRead(s.store.CurrentRev())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer reader.Close()
	vv, err := reader.GetAt(key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": string(vv.Value), "rev": vv.Rev})
}

type leaseRequest struct {
	ID     model.LeaseID `json:"id"`
	TTLMS  int64         `json:"ttl_ms,omitempty"`
	Holder string        `json:"holder,omitempty"`
}

func (s *Server) handleLeaseCreate(w http.ResponseWriter, r *http.Request) {
	var req leaseRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	ttl := s.cfg.LeaseTTL
	if req.TTLMS > 0 {
		ttl = time.Duration(req.TTLMS) * time.Millisecond
	}
	if err := s.leases.Create(req.ID, ttl, req.Holder); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": req.ID, "ttl": ttl.String()})
}

func (s *Server) handleLeaseRenew(w http.ResponseWriter, r *http.Request) {
	var req leaseRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := s.leases.Renew(req.ID, nowOf(s)); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": req.ID, "renewed": true})
}

func (s *Server) handleLeaseBatchRenew(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []model.LeaseID `json:"ids"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	results := s.leases.BatchRenew(req.IDs, nowOf(s))
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (s *Server) handleLeaseRevoke(w http.ResponseWriter, r *http.Request) {
	var req leaseRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := s.leases.Revoke(req.ID); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": req.ID, "revoked": true})
}

func (s *Server) handleLeaseGet(w http.ResponseWriter, r *http.Request) {
	id := model.LeaseID(strings.TrimPrefix(r.URL.Path, "/api/lease/"))
	if id == "" {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "lease not found"})
		return
	}
	l, ok := s.leases.Lookup(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "lease not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":         l.ID,
		"holder":     l.Holder,
		"ttl":        l.TTL.String(),
		"expires_at": l.ExpiresAt.Format(time.RFC3339Nano),
		"renew_seq":  l.RenewSeq,
		"state":      l.State.String(),
		"keys":       s.leases.KeysOf(id),
	})
}

type lockRequest struct {
	ID        model.LockID  `json:"id"`
	Client    string        `json:"client"`
	LeaseID   model.LeaseID `json:"lease_id,omitempty"`
	TimeoutMS int64         `json:"timeout_ms,omitempty"`
}

func (s *Server) handleLockAcquire(w http.ResponseWriter, r *http.Request) {
	var req lockRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	timeout := 5 * time.Second
	if req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	if err := s.locks.Acquire(ctx, req.ID, req.Client, req.LeaseID, nowOf(s)); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": req.ID, "client": req.Client, "acquired": true})
}

func (s *Server) handleLockRelease(w http.ResponseWriter, r *http.Request) {
	var req lockRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := s.locks.Release(req.ID, req.Client, nowOf(s)); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": req.ID, "released": true})
}

func (s *Server) handleLockSnapshot(w http.ResponseWriter, _ *http.Request) {
	records := s.locks.Snapshot()
	view := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		view = append(view, map[string]any{
			"id":     rec.ID,
			"holder": rec.Holder,
			"lease":  rec.Lease,
			"state":  rec.State.String(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"locks": view})
}

// ssePeer writes watch events to an HTTP client as server-sent events.
type ssePeer struct {
	w      http.ResponseWriter
	closed chan struct{}
	once   sync.Once
}

func (p *ssePeer) Send(_ model.WatchID, ev model.Event) error {
	select {
	case <-p.closed:
		return errors.New("watch: peer closed")
	default:
	}
	payload, _ := json.Marshal(map[string]any{
		"type":  ev.Type.String(),
		"key":   ev.Key,
		"value": string(ev.Value),
		"rev":   ev.Rev,
	})
	if _, err := fmt.Fprintf(p.w, "data: %s\n\n", payload); err != nil {
		return err
	}
	if flusher, ok := p.w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func (p *ssePeer) Close(_ model.WatchID) {
	p.once.Do(func() {
		close(p.closed)
	})
}

func (s *Server) handleWatch(w http.ResponseWriter, r *http.Request) {
	sessionID := model.SessionID(r.URL.Query().Get("session"))
	key := r.URL.Query().Get("key")
	start := model.Revision(0)
	if raw := r.URL.Query().Get("rev"); raw != "" {
		if parsed, err := strconv.ParseUint(raw, 10, 64); err == nil {
			start = model.Revision(parsed)
		}
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	watchID, _, err := s.sessions.RegisterWatch(sessionID, key, start)
	if err != nil {
		return
	}
	peer := &ssePeer{w: w, closed: make(chan struct{})}
	defer peer.Close(watchID)
	defer s.watcher.Cancel(watchID)
	s.sessions.AttachPeer(sessionID, watchID, peer)
	<-r.Context().Done()
}

type sessionRequest struct {
	ID      model.SessionID `json:"id"`
	LeaseID model.LeaseID   `json:"lease_id,omitempty"`
}

func (s *Server) handleSessionConnect(w http.ResponseWriter, r *http.Request) {
	var req sessionRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := s.sessions.Connect(req.ID); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	if req.LeaseID != "" {
		_ = s.sessions.BindLease(req.ID, req.LeaseID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": req.ID, "connected": true})
}

func (s *Server) handleSessionReconnect(w http.ResponseWriter, r *http.Request) {
	var req sessionRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if _, err := s.sessions.Reconnect(req.ID); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": req.ID, "reconnected": true})
}

func (s *Server) handleSessionStatus(w http.ResponseWriter, r *http.Request) {
	id := model.SessionID(r.URL.Query().Get("id"))
	if id != "" {
		sess, ok := s.sessions.Lookup(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "session not found"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id":            sess.ID,
			"state":         sess.State.String(),
			"leases":        sess.LeaseIDs,
			"watch_cursors": sess.Watches,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"connected":    s.sessions.SessionsByState(model.SessionConnected),
		"disconnected": s.sessions.SessionsByState(model.SessionDisconnected),
	})
}

func (s *Server) handleSessionResumeAll(w http.ResponseWriter, _ *http.Request) {
	ids := s.sessions.SessionsByState(model.SessionDisconnected)
	restored := s.sessions.ResumeAll(ids)
	writeJSON(w, http.StatusOK, map[string]any{"restored": restored})
}

type electionRequest struct {
	Name    string     `json:"name"`
	NewTerm model.Term `json:"term,omitempty"`
	NewName string     `json:"new_name,omitempty"`
}

func (s *Server) handleElectionCampaign(w http.ResponseWriter, r *http.Request) {
	var req electionRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	name := req.Name
	if name == "" {
		name = s.cfg.NodeName
	}
	if err := s.election.Campaign(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	role, term, leader := s.election.Current()
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "role": role.String(), "term": term, "leader": leader})
}

func (s *Server) handleElectionHandoff(w http.ResponseWriter, r *http.Request) {
	var req electionRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if req.NewName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "new_name is required"})
		return
	}
	if req.NewTerm == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "term is required"})
		return
	}
	if err := s.election.Handoff(req.NewTerm, req.NewName); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	role, term, leader := s.election.Current()
	writeJSON(w, http.StatusOK, map[string]any{"role": role.String(), "term": term, "leader": leader})
}

func (s *Server) handleElectionResign(w http.ResponseWriter, _ *http.Request) {
	s.election.Resign()
	s.election.Follow()
	writeJSON(w, http.StatusOK, map[string]any{"resigned": true})
}

func (s *Server) handleElectionStatus(w http.ResponseWriter, _ *http.Request) {
	role, term, leader := s.election.Current()
	writeJSON(w, http.StatusOK, map[string]any{
		"role":         role.String(),
		"term":         term,
		"leader":       leader,
		"leader_lease": election.LeaseNameFor(leader),
		"heartbeat_ok": !s.election.CheckTimeout(),
	})
}

var _ watch.Peer = (*ssePeer)(nil)
