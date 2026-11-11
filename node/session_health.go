/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"container/list"
	"log"
	"sync"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/relay"
)

// sessionEntry wraps a session with LRU tracking
type sessionEntry struct {
	session  aether.Connection
	nodeID   aether.NodeID
	lastUsed time.Time
}

// SessionHealthMonitor holds the sessions Runtime.RegisterSession hands it (the QUIC
// fallback listener's sessions and the external relay sessions RegisterExternalSession
// adds), closes one that reports itself closed or idle past IdleTimeout, and evicts the
// least recently active one past MaxSessions. It sends no PING and keeps no missed-ping
// verdict: a session's liveness probe is its one keepalive (runMeshKeepalive on the mesh
// session AcceptMeshConnection runs over it, aether.RunKeepalive for a session held
// outside a node runtime), whose plan quality.Keepalive derives from that session's
// measurements, and health.Monitor holds one pending probe sequence, so a PING from
// here would replace the keepalive's and take its answer.
type SessionHealthMonitor struct {
	config   relay.HealthConfig
	sessions map[aether.NodeID]*list.Element // Map to LRU list element
	lruList  *list.List                      // LRU ordering (front = most recent)
	mu       sync.RWMutex

	// Callbacks
	onEvict func(nodeID aether.NodeID, reason string)

	// Lifecycle
	stopChan chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once // Stop closes stopChan; a second bare close() panics
}

// NewSessionHealthMonitor creates a new session health monitor with the given config.
func NewSessionHealthMonitor(config relay.HealthConfig) *SessionHealthMonitor {
	return &SessionHealthMonitor{
		config:   config,
		sessions: make(map[aether.NodeID]*list.Element),
		lruList:  list.New(),
		stopChan: make(chan struct{}),
	}
}

// SetEvictCallback sets the callback invoked when a session is evicted.
func (m *SessionHealthMonitor) SetEvictCallback(cb func(nodeID aether.NodeID, reason string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onEvict = cb
}

// Register adds a session to be monitored.
// If at capacity (MaxSessions), the least recently used session is evicted.
func (m *SessionHealthMonitor) Register(session aether.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodeID := session.RemoteNodeID()

	// Check if already registered - update LRU position
	if elem, exists := m.sessions[nodeID]; exists {
		entry := elem.Value.(*sessionEntry)
		entry.session = session
		entry.lastUsed = time.Now()
		m.lruList.MoveToFront(elem)
		dbgSessionHealth.Printf("Updated session for node %s", nodeID)
		return
	}

	// Check capacity and evict LRU if needed
	if m.config.MaxSessions > 0 && m.lruList.Len() >= m.config.MaxSessions {
		m.evictLRULocked()
	}

	// Add new session to front of LRU list
	entry := &sessionEntry{
		session:  session,
		nodeID:   nodeID,
		lastUsed: time.Now(),
	}
	elem := m.lruList.PushFront(entry)
	m.sessions[nodeID] = elem
	dbgSessionHealth.Printf("Registered session for node %s (total: %d)", nodeID, m.lruList.Len())
}

// Touch updates the LRU position for a session (call on activity).
func (m *SessionHealthMonitor) Touch(nodeID aether.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if elem, exists := m.sessions[nodeID]; exists {
		entry := elem.Value.(*sessionEntry)
		entry.lastUsed = time.Now()
		m.lruList.MoveToFront(elem)
	}
}

// Unregister removes a session from monitoring.
func (m *SessionHealthMonitor) Unregister(nodeID aether.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if elem, exists := m.sessions[nodeID]; exists {
		m.lruList.Remove(elem)
		delete(m.sessions, nodeID)
		dbgSessionHealth.Printf("Unregistered session for node %s (total: %d)", nodeID, m.lruList.Len())
	}
}

// evictionCandidateLocked picks which session to drop at capacity. Must hold mu.
//
// Eviction is decided by the session's own LastActivity(), not by
// entry.lastUsed. There were two definitions of "recently active" in this one
// monitor — entry.lastUsed for eviction and LastActivity() for health — and
// lastUsed was written three times and read once, in a debug log, because Touch
// has no callers, so an "LRU" list built from it is really registration order.
//
// 🔴 THE DISCRIMINATOR IS THE LOAD-BEARING PART. A BaseConnection over a raw
// net.Conn returns a ZERO LastActivity, which means "CANNOT SAY", not "idle
// since 1970". Sorting naively by LastActivity would therefore evict every
// WebSocket/gRPC relay session FIRST — precisely the fallback transports
// the health guard exists to protect, and precisely the churn it was
// written to stop. checkSession already makes this distinction 120 lines below
// (`!hr.LastActivity().IsZero()`); the eviction path never got it.
//
// So: evict the oldest session that CAN report activity. Only if no session can
// report at all do we fall back to registration order (the list back) among the
// cannot-say set.
func (m *SessionHealthMonitor) evictionCandidateLocked() *list.Element {
	var oldest *list.Element
	var oldestAt time.Time

	for elem := m.lruList.Back(); elem != nil; elem = elem.Prev() {
		hr, ok := elem.Value.(*sessionEntry).session.(aether.HealthReporter)
		if !ok {
			continue // cannot report — not a candidate while anything else can
		}
		at := hr.LastActivity()
		if at.IsZero() {
			continue // "cannot say", not "stale"
		}
		if oldest == nil || at.Before(oldestAt) {
			oldest, oldestAt = elem, at
		}
	}
	if oldest != nil {
		return oldest
	}
	// Nothing could report activity: every session is "cannot say", so fall back
	// to registration order among them — the back of the list, as before.
	return m.lruList.Back()
}

// evictLRULocked evicts the least recently used session. Must hold mu.
func (m *SessionHealthMonitor) evictLRULocked() {
	victim := m.evictionCandidateLocked()
	if victim == nil {
		return
	}

	entry := victim.Value.(*sessionEntry)
	nodeID := entry.nodeID
	session := entry.session

	// Remove from LRU and map
	m.lruList.Remove(victim)
	delete(m.sessions, nodeID)

	idle := "unknown (session cannot report activity)"
	if hr, ok := session.(aether.HealthReporter); ok {
		if at := hr.LastActivity(); !at.IsZero() {
			idle = time.Since(at).String()
		}
	}
	dbgSessionHealth.Printf("LRU evicting session for node %s (idle: %s)", nodeID, idle)

	// Snapshot onEvict under m.mu (held by this Locked caller) before
	// launching the goroutine. Reading m.onEvict inside the goroutine raced
	// SetEvictCallback's write; evictSession already snapshots it under the lock.
	onEvict := m.onEvict
	// Close session outside lock to avoid deadlock
	go func() {
		if err := session.Close(); err != nil {
			log.Printf("[SESSION_HEALTH] Error closing LRU-evicted session for %s: %v", nodeID, err)
		}
		if onEvict != nil {
			onEvict(nodeID, "LRU capacity eviction")
		}
	}()
}

// Start begins the health monitoring loop.
func (m *SessionHealthMonitor) Start() {
	m.wg.Add(1)
	go m.monitorLoop()
	log.Printf("[SESSION_HEALTH] Monitor started (sweep interval: %v, idle timeout: %v, max sessions: %d)",
		m.config.PingInterval, m.config.IdleTimeout, m.config.MaxSessions)
}

// Stop terminates the health monitor. Idempotent.
//
// The sync.Once is load-bearing: the previous bare `close(m.stopChan)` panicked
// with "close of closed channel" on a second call (measured with a
// probe). Runtime only calls Stop inside shutdownOnce-guarded Shutdown, so it
// was not reachable there — but Stop is exported on a published module, and the
// sibling LADSnapshotCache.Stop 200 lines away already guarded itself this way.
func (m *SessionHealthMonitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.wg.Wait()
		log.Printf("[SESSION_HEALTH] Monitor stopped")
	})
}

// ActiveCount returns the number of active sessions.
func (m *SessionHealthMonitor) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// monitorLoop sweeps every session once per config.PingInterval (checkAllSessions).
// The sweep sends nothing to a peer; the interval only bounds how long a closed or
// idle session holds its slot.
func (m *SessionHealthMonitor) monitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.checkAllSessions()
		}
	}
}

// checkAllSessions iterates all sessions and performs health checks.
func (m *SessionHealthMonitor) checkAllSessions() {
	m.mu.RLock()
	entries := make([]*sessionEntry, 0, m.lruList.Len())
	for elem := m.lruList.Front(); elem != nil; elem = elem.Next() {
		entries = append(entries, elem.Value.(*sessionEntry))
	}
	m.mu.RUnlock()

	var toEvict []struct {
		nodeID aether.NodeID
		reason string
	}

	for _, entry := range entries {
		reason := m.checkSession(entry.session)
		if reason != "" {
			toEvict = append(toEvict, struct {
				nodeID aether.NodeID
				reason string
			}{entry.nodeID, reason})
		}
	}

	// Evict unhealthy sessions
	for _, e := range toEvict {
		m.evictSession(e.nodeID, e.reason)
	}
}

// checkSession returns the reason checkAllSessions evicts session for, or "" to keep
// it: a session that reports itself closed, or one that tracks activity and has been
// idle past IdleTimeout. A session that reports no health is kept. It sends no PING
// and reads no missed-ping count; the session's keepalive lets a silent peer go.
func (m *SessionHealthMonitor) checkSession(session aether.Connection) string {
	hr, ok := session.(aether.HealthReporter)
	if !ok {
		return "" // Session doesn't support health reporting — skip
	}

	if hr.IsClosed() {
		return "session closed"
	}
	// IsAlive() returns false in TWO cases: (a) the session genuinely
	// idle-timed out, and (b) the underlying transport can't report health at
	// all. A WebSocket/gRPC relay session is an aether.BaseConnection wrapping a
	// raw net.Conn; BaseConnection.IsAlive returns false and LastActivity returns
	// the zero time whenever the inner conn is not itself a HealthReporter. An
	// unconditional check therefore evicts EVERY healthy WS/gRPC relay session
	// on each PingInterval, which is pure reconnect churn on exactly the
	// fallback transports. Only trust IsAlive==false as an idle timeout when the session
	// actually tracks activity (non-zero LastActivity); otherwise keep it, matching the
	// non-HealthReporter skip above.
	if !hr.LastActivity().IsZero() && !hr.IsAlive(m.config.IdleTimeout) {
		return "idle timeout exceeded"
	}
	return ""
}

// evictSession removes and closes an unhealthy session.
func (m *SessionHealthMonitor) evictSession(nodeID aether.NodeID, reason string) {
	m.mu.Lock()
	elem, exists := m.sessions[nodeID]
	var session aether.Connection
	if exists {
		entry := elem.Value.(*sessionEntry)
		session = entry.session
		m.lruList.Remove(elem)
		delete(m.sessions, nodeID)
	}
	onEvict := m.onEvict
	m.mu.Unlock()

	if !exists {
		return
	}

	log.Printf("[SESSION_HEALTH] Evicting session for node %s: %s", nodeID, reason)

	// Close the session
	if err := session.Close(); err != nil {
		log.Printf("[SESSION_HEALTH] Error closing session for %s: %v", nodeID, err)
	}

	// Invoke callback
	if onEvict != nil {
		onEvict(nodeID, reason)
	}
}
