/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package manager

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
)

var (
	ErrTenantNotRegistered = errors.New("manager: tenant transport not registered")
	ErrTenantAlreadyExists = errors.New("manager: tenant transport already registered")
	ErrManagerClosed       = errors.New("manager: transport manager is closed")
)

// TransportManager routes transport operations to per-tenant NoiseTransport instances.
// It sits between Runtime and the transport layer, providing tenant-aware Dial/Listen.
type TransportManager struct {
	mu         sync.RWMutex
	transports map[aether.ScopeID]aether.Transport
	closed     bool
}

// New creates a new TransportManager.
func New() *TransportManager {
	return &TransportManager{
		transports: make(map[aether.ScopeID]aether.Transport),
	}
}

// Register adds a transport for a tenant. Returns ErrTenantAlreadyExists
// if the tenant is already registered.
func (m *TransportManager) Register(tenantID aether.ScopeID, t aether.Transport) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.transports[tenantID]; exists {
		return ErrTenantAlreadyExists
	}

	m.transports[tenantID] = t
	return nil
}

// Replace swaps the transport for an existing tenant (used for key rotation).
// Returns ErrTenantNotRegistered if the tenant doesn't exist.
func (m *TransportManager) Replace(tenantID aether.ScopeID, t aether.Transport) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.transports[tenantID]; !exists {
		return ErrTenantNotRegistered
	}

	m.transports[tenantID] = t
	return nil
}

// Remove deregisters a tenant's aether. Does not close the aether.
// Returns ErrTenantNotRegistered if the tenant doesn't exist.
func (m *TransportManager) Remove(tenantID aether.ScopeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.transports[tenantID]; !exists {
		return ErrTenantNotRegistered
	}

	delete(m.transports, tenantID)
	return nil
}

// Get returns the transport for a tenant.
func (m *TransportManager) Get(tenantID aether.ScopeID) (aether.Transport, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrManagerClosed
	}
	t, ok := m.transports[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTenantNotRegistered, tenantID)
	}
	return t, nil
}

// Dial establishes a session to a remote node through the tenant's aether.
// The returned session is wrapped as a TenantSession carrying the tenant ID.
// The tenant ID is injected into the context so shared transports can use it
// for preamble construction during the handshake.
func (m *TransportManager) Dial(ctx context.Context, tenantID aether.ScopeID, target aether.Target) (*TenantSession, error) {
	t, err := m.Get(tenantID)
	if err != nil {
		return nil, err
	}

	// Inject tenant ID into context for shared transport preamble support.
	ctx = WithTenantID(ctx, tenantID)

	session, err := t.Dial(ctx, target)
	if err != nil {
		return nil, err
	}

	return &TenantSession{
		Connection: session,
		tenantID: tenantID,
	}, nil
}

// tenantIDContextKey is the context key for passing tenant ID through Dial.
type tenantIDContextKey struct{}

// WithTenantID injects a tenant ID into context.
func WithTenantID(ctx context.Context, id aether.ScopeID) context.Context {
	return context.WithValue(ctx, tenantIDContextKey{}, id)
}

// TenantIDFromContext extracts a tenant ID from context.
func TenantIDFromContext(ctx context.Context) (aether.ScopeID, bool) {
	id, ok := ctx.Value(tenantIDContextKey{}).(aether.ScopeID)
	return id, ok
}

// ListTenants returns all registered tenant IDs.
func (m *TransportManager) ListTenants() []aether.ScopeID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]aether.ScopeID, 0, len(m.transports))
	for id := range m.transports {
		ids = append(ids, id)
	}
	return ids
}

// Count returns the number of registered tenants.
func (m *TransportManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.transports)
}

// ManagerStats is a point-in-time snapshot of TransportManager state.
// Surfaces per-tenant transport type breakdown so operators can tell at
// a glance which tenants are on which transport family and whether any
// are running on a degraded path.
type ManagerStats struct {
	Closed       bool
	TenantCount  int
	Tenants      []TenantStats  // per-tenant breakdown, sorted by tenant ID
	ByTransport  map[string]int // transport-type-name → tenant count
}

// TenantStats is per-tenant observable state.
type TenantStats struct {
	TenantID      aether.ScopeID
	TransportType string // Go type name of the registered transport
	Protocol      string // aether.Protocol name if available
}

// Stats returns a full snapshot for diagnostics endpoints. Cheap — one
// RLock + O(tenants) string formatting.
func (m *TransportManager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := ManagerStats{
		Closed:      m.closed,
		TenantCount: len(m.transports),
		Tenants:     make([]TenantStats, 0, len(m.transports)),
		ByTransport: make(map[string]int),
	}
	for tid, t := range m.transports {
		typeName := fmt.Sprintf("%T", t)
		proto := ""
		if prov, ok := t.(interface{ Protocol() aether.Protocol }); ok {
			proto = prov.Protocol().String()
		}
		out.Tenants = append(out.Tenants, TenantStats{
			TenantID:      tid,
			TransportType: typeName,
			Protocol:      proto,
		})
		out.ByTransport[typeName]++
	}
	return out
}

// RotateKey prepends a new key to the tenant's key list, beginning dual-key overlap.
// The new key becomes the active key for outbound handshakes; old keys remain
// accepted for inbound handshakes until FinalizeRotation is called.
func (m *TransportManager) RotateKey(tenantID aether.ScopeID, newKey []byte) error {
	t, err := m.Get(tenantID)
	if err != nil {
		return err
	}

	keyCopy := append([]byte(nil), newKey...)

	switch tx := t.(type) {
	case *SharedTransport:
		currentKeys, err := tx.ResolveKeys(string(tenantID))
		if err != nil {
			return fmt.Errorf("resolve current keys: %w", err)
		}
		newKeys := append([][]byte{keyCopy}, currentKeys...)
		return tx.UpdateTenantKeys(tenantID, newKeys)

	case *noise.NoiseTransport:
		currentKeys := tx.CurrentKeys()
		newKeys := append([][]byte{keyCopy}, currentKeys...)
		return tx.RotateKeys(newKeys)

	default:
		return fmt.Errorf("transport type %T does not support key rotation", t)
	}
}

// FinalizeRotation removes all old keys, keeping only the current (first) key.
// Call this after the overlap period has elapsed to end dual-key acceptance.
func (m *TransportManager) FinalizeRotation(tenantID aether.ScopeID) error {
	t, err := m.Get(tenantID)
	if err != nil {
		return err
	}

	switch tx := t.(type) {
	case *SharedTransport:
		currentKeys, err := tx.ResolveKeys(string(tenantID))
		if err != nil {
			return fmt.Errorf("resolve current keys: %w", err)
		}
		if len(currentKeys) <= 1 {
			return nil // Already finalized
		}
		return tx.UpdateTenantKeys(tenantID, currentKeys[:1])

	case *noise.NoiseTransport:
		currentKeys := tx.CurrentKeys()
		if len(currentKeys) <= 1 {
			return nil
		}
		return tx.RotateKeys(currentKeys[:1])

	default:
		return fmt.Errorf("transport type %T does not support key rotation", t)
	}
}

// Close shuts down all registered transports and marks the manager as closed.
func (m *TransportManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	var firstErr error
	for id, t := range m.transports {
		if err := t.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close transport for tenant %s: %w", id, err)
		}
	}
	m.transports = nil
	return firstErr
}
