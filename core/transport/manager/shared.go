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
	ErrSharedTenantNotFound   = errors.New("shared: tenant not registered")
	ErrSharedTenantExists     = errors.New("shared: tenant already registered")
	ErrSharedNoTenantInCtx    = errors.New("shared: no tenant ID in context (use TransportManager.Dial)")
	ErrSharedNoKeysForTenant  = errors.New("shared: no keys registered for tenant")
)

// SharedTransport wraps a single NoiseTransport to serve multiple platform
// tenants on the same UDP port. It uses the preamble protocol to identify
// which tenant a connection belongs to, allowing per-tenant PSK selection.
//
// On the initiator side, SharedTransport reads the tenant ID from context
// (injected by TransportManager.Dial) and passes the tenant's keys to the
// underlying handshake via WithDialOverride.
//
// On the listener side, the underlying NoiseTransport uses SharedTransport
// as its TenantKeyResolver to dynamically resolve keys from the preamble.
type SharedTransport struct {
	inner *noise.NoiseTransport

	mu         sync.RWMutex
	tenantKeys map[aether.ScopeID][][]byte // tenant → PSK keys (first = active)
}

// NewSharedTransport creates a shared transport backed by a NoiseTransport
// configured with this SharedTransport as the TenantKeyResolver.
//
// The config's NetworkKeys field is used as a default/fallback for the
// transport itself (e.g., for STUN). The TenantKeyResolver is set automatically.
func NewSharedTransport(cfg noise.NoiseTransportConfig) (*SharedTransport, error) {
	st := &SharedTransport{
		tenantKeys: make(map[aether.ScopeID][][]byte),
	}

	// Install this SharedTransport as the key resolver for preamble mode.
	cfg.TenantKeyResolver = st

	inner, err := noise.NewNoiseTransport(cfg)
	if err != nil {
		return nil, fmt.Errorf("shared: create transport: %w", err)
	}
	st.inner = inner
	return st, nil
}

// RegisterTenant adds a tenant with its network keys. The first key is used
// for outbound connections (active key); additional keys are accepted for
// inbound connections (key rotation overlap).
func (s *SharedTransport) RegisterTenant(tenantID aether.ScopeID, keys [][]byte) error {
	if len(keys) == 0 {
		return ErrSharedNoKeysForTenant
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tenantKeys[tenantID]; exists {
		return ErrSharedTenantExists
	}
	// Deep copy keys to prevent external mutation.
	copied := make([][]byte, len(keys))
	for i, k := range keys {
		copied[i] = append([]byte(nil), k...)
	}
	s.tenantKeys[tenantID] = copied
	return nil
}

// UpdateTenantKeys replaces the keys for an existing tenant (used for key rotation).
func (s *SharedTransport) UpdateTenantKeys(tenantID aether.ScopeID, keys [][]byte) error {
	if len(keys) == 0 {
		return ErrSharedNoKeysForTenant
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tenantKeys[tenantID]; !exists {
		return ErrSharedTenantNotFound
	}
	copied := make([][]byte, len(keys))
	for i, k := range keys {
		copied[i] = append([]byte(nil), k...)
	}
	s.tenantKeys[tenantID] = copied
	return nil
}

// RemoveTenant deregisters a tenant from the shared aether.
func (s *SharedTransport) RemoveTenant(tenantID aether.ScopeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tenantKeys[tenantID]; !exists {
		return ErrSharedTenantNotFound
	}
	delete(s.tenantKeys, tenantID)
	return nil
}

// TenantCount returns the number of registered tenants.
func (s *SharedTransport) TenantCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tenantKeys)
}

// ResolveKeys implements noise.TenantKeyResolver for the listener's preamble handling.
func (s *SharedTransport) ResolveKeys(tenantID string) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys, ok := s.tenantKeys[aether.ScopeID(tenantID)]
	if !ok || len(keys) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrSharedTenantNotFound, tenantID)
	}
	return keys, nil
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// aether.Transport implementation
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// Dial establishes a session with preamble for the tenant identified in context.
// The context must contain a tenant ID (set by TransportManager.Dial via WithTenantID).
func (s *SharedTransport) Dial(ctx context.Context, target aether.Target) (aether.Connection, error) {
	tenantID, ok := TenantIDFromContext(ctx)
	if !ok || tenantID == "" {
		return nil, ErrSharedNoTenantInCtx
	}

	s.mu.RLock()
	keys, exists := s.tenantKeys[tenantID]
	s.mu.RUnlock()
	if !exists || len(keys) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrSharedTenantNotFound, tenantID)
	}

	// Inject the tenant's keys and ID into context so the underlying
	// NoiseTransport handshake prepends the preamble.
	ctx = noise.WithDialOverride(ctx, string(tenantID), keys)
	return s.inner.Dial(ctx, target)
}

// Listen delegates to the underlying NoiseTransport. Preamble handling
// is done inside the noise package's handleHandshake via TenantKeyResolver.
func (s *SharedTransport) Listen(ctx context.Context) (aether.Listener, error) {
	return s.inner.Listen(ctx)
}

// Inner returns the underlying NoiseTransport for direct access (e.g., UDP dial).
func (s *SharedTransport) Inner() *noise.NoiseTransport {
	return s.inner
}

// Close shuts down the underlying aether.
func (s *SharedTransport) Close() error {
	return s.inner.Close()
}
