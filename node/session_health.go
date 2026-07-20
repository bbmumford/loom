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

// SessionHealthMonitor tracks and monitors active VL1 sessions for health.
// It sends periodic pings, detects idle sessions, evicts unhealthy ones,
// and performs LRU eviction when at capacity.
type SessionHealthMonitor struct {
	config   relay.HealthConfig
	sessions map[aether.NodeID]*list.Element // Map to LRU list element
	lruList  *list.List                         // LRU ordering (front = most recent)
	mu       sync.RWMutex

	// Callbacks
	onEvict func(nodeID aether.NodeID, reason string)

	// Lifecycle
	stopChan chan struct{}
	wg       sync.WaitGroup
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

// evictLRULocked evicts the least recently used session. Must hold mu.
func (m *SessionHealthMonitor) evictLRULocked() {
	back := m.lruList.Back()
	if back == nil {
		return
	}

	entry := back.Value.(*sessionEntry)
	nodeID := entry.nodeID
	session := entry.session

	// Remove from LRU and map
	m.lruList.Remove(back)
	delete(m.sessions, nodeID)

	dbgSessionHealth.Printf("LRU evicting session for node %s (idle: %v)", nodeID, time.Since(entry.lastUsed))

	// MESH-C08: snapshot onEvict under m.mu (held by this Locked caller) before
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
	log.Printf("[SESSION_HEALTH] Monitor started (ping interval: %v, idle timeout: %v, max missed: %d)",
		m.config.PingInterval, m.config.IdleTimeout, m.config.MaxMissedPings)
}

// Stop terminates the health monitor.
func (m *SessionHealthMonitor) Stop() {
	close(m.stopChan)
	m.wg.Wait()
	log.Printf("[SESSION_HEALTH] Monitor stopped")
}

// ActiveCount returns the number of active sessions.
func (m *SessionHealthMonitor) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// monitorLoop runs periodically to check session health.
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

// checkSession performs health check on a single session.
// Returns eviction reason if session should be evicted, empty string if healthy.
func (m *SessionHealthMonitor) checkSession(session aether.Connection) string {
	hr, ok := session.(aether.HealthReporter)
	if !ok {
		return "" // Session doesn't support health reporting — skip
	}

	nodeID := session.RemoteNodeID()

	if hr.IsClosed() {
		return "session closed"
	}
	// MESH-G01: IsAlive() returns false in TWO cases: (a) the session genuinely
	// idle-timed out, and (b) the underlying transport can't report health at
	// all. A WebSocket/gRPC relay session is an aether.BaseConnection wrapping a
	// raw net.Conn; BaseConnection.IsAlive returns false and LastActivity returns
	// the zero time whenever the inner conn is not itself a HealthReporter. So
	// the old unconditional check evicted EVERY healthy WS/gRPC relay session on
	// each PingInterval — pure reconnect churn on exactly the fallback
	// transports. Only trust IsAlive==false as an idle timeout when the session
	// actually tracks activity (non-zero LastActivity); otherwise fall through to
	// the ping-based liveness below, matching the non-HealthReporter skip above.
	if !hr.LastActivity().IsZero() && !hr.IsAlive(m.config.IdleTimeout) {
		return "idle timeout exceeded"
	}
	if hr.MissedPings() >= m.config.MaxMissedPings {
		return "max missed pings exceeded"
	}

	// Send ping if overdue (only if session supports it)
	lastPong := hr.LastPongReceived()
	if time.Since(lastPong) > m.config.PingInterval {
		p, pingOK := session.(aether.Pingable)
		if !pingOK {
			return "" // Can't ping — rely on activity-based health only
		}
		seq, err := p.SendPing()
		if err != nil {
			log.Printf("[SESSION_HEALTH] Failed to send ping to %s: %v", nodeID, err)
			missed := p.IncrementMissedPings()
			if missed >= m.config.MaxMissedPings {
				return "ping send failures"
			}
		} else {
			dbgSessionHealth.Printf("Sent ping seq=%d to %s", seq, nodeID)
		}
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
