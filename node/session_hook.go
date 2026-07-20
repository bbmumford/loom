/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "github.com/ORBTR/aether"

// SetSessionHook installs a callback invoked when ConnectionManager
// installs or removes a session in its dispatch table. The hook fires
// after the lock is released and outside any critical path — handlers
// may safely call back into the manager.
//
// On registration: session is non-nil and joined is true.
// On unregistration: session is nil and joined is false.
//
// Used by the swarm transport adapter to attach/detach peer streams
// in lockstep with the underlying session lifecycle.
func (m *ConnectionManager) SetSessionHook(fn func(nodeID string, session aether.Session, joined bool)) {
	m.sessionHookMu.Lock()
	defer m.sessionHookMu.Unlock()
	m.sessionHook = fn
}

func (m *ConnectionManager) fireSessionHook(nodeID string, session aether.Session, joined bool) {
	m.sessionHookMu.RLock()
	fn := m.sessionHook
	m.sessionHookMu.RUnlock()
	if fn != nil {
		fn(nodeID, session, joined)
	}
}
