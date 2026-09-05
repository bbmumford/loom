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
	ErrSharedPartitionNotFound  = errors.New("shared: partition not registered")
	ErrSharedPartitionExists    = errors.New("shared: partition already registered")
	ErrSharedNoPartitionInCtx   = errors.New("shared: no partition ID in context (use TransportManager.Dial)")
	ErrSharedNoKeysForPartition = errors.New("shared: no keys registered for partition")
)

// SharedTransport wraps a single NoiseTransport to serve multiple platform
// partitions on the same UDP port. It uses the preamble protocol to identify
// which partition a connection belongs to, allowing per-partition PSK selection.
//
// On the initiator side, SharedTransport reads the partition ID from context
// (injected by TransportManager.Dial) and passes the partition's keys to the
// underlying handshake via WithDialOverride.
//
// On the listener side, the underlying NoiseTransport uses SharedTransport
// as its TenantKeyResolver to dynamically resolve keys from the preamble.
type SharedTransport struct {
	inner *noise.NoiseTransport

	mu         sync.RWMutex
	partitionKeys map[aether.ScopeID][][]byte // partition → PSK keys (first = active)
}

// NewSharedTransport creates a shared transport backed by a NoiseTransport
// configured with this SharedTransport as the TenantKeyResolver.
//
// The config's NetworkKeys field is used as a default/fallback for the
// transport itself (e.g., for STUN). The TenantKeyResolver is set automatically.
func NewSharedTransport(cfg noise.NoiseTransportConfig) (*SharedTransport, error) {
	st := &SharedTransport{
		partitionKeys: make(map[aether.ScopeID][][]byte),
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

// RegisterPartition adds a partition with its network keys. The first key is used
// for outbound connections (active key); additional keys are accepted for
// inbound connections (key rotation overlap).
func (s *SharedTransport) RegisterPartition(partitionID aether.ScopeID, keys [][]byte) error {
	if len(keys) == 0 {
		return ErrSharedNoKeysForPartition
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.partitionKeys[partitionID]; exists {
		return ErrSharedPartitionExists
	}
	// Deep copy keys to prevent external mutation.
	copied := make([][]byte, len(keys))
	for i, k := range keys {
		copied[i] = append([]byte(nil), k...)
	}
	s.partitionKeys[partitionID] = copied
	return nil
}

// UpdatePartitionKeys replaces the keys for an existing partition (used for key rotation).
func (s *SharedTransport) UpdatePartitionKeys(partitionID aether.ScopeID, keys [][]byte) error {
	if len(keys) == 0 {
		return ErrSharedNoKeysForPartition
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.partitionKeys[partitionID]; !exists {
		return ErrSharedPartitionNotFound
	}
	copied := make([][]byte, len(keys))
	for i, k := range keys {
		copied[i] = append([]byte(nil), k...)
	}
	s.partitionKeys[partitionID] = copied
	return nil
}

// RemovePartition deregisters a partition from the shared aether.
func (s *SharedTransport) RemovePartition(partitionID aether.ScopeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.partitionKeys[partitionID]; !exists {
		return ErrSharedPartitionNotFound
	}
	delete(s.partitionKeys, partitionID)
	return nil
}

// PartitionCount returns the number of registered partitions.
func (s *SharedTransport) PartitionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.partitionKeys)
}

// ResolveKeys implements noise.TenantKeyResolver for the listener's preamble handling.
func (s *SharedTransport) ResolveKeys(partitionID string) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys, ok := s.partitionKeys[aether.ScopeID(partitionID)]
	if !ok || len(keys) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrSharedPartitionNotFound, partitionID)
	}
	return keys, nil
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// aether.Transport implementation
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// Dial establishes a session with preamble for the tenant identified in context.
// The context must contain a tenant ID (set by TransportManager.Dial via WithPartitionID).
func (s *SharedTransport) Dial(ctx context.Context, target aether.Target) (aether.Connection, error) {
	partitionID, ok := PartitionIDFromContext(ctx)
	if !ok || partitionID == "" {
		return nil, ErrSharedNoPartitionInCtx
	}

	s.mu.RLock()
	keys, exists := s.partitionKeys[partitionID]
	s.mu.RUnlock()
	if !exists || len(keys) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrSharedPartitionNotFound, partitionID)
	}

	// Inject the tenant's keys and ID into context so the underlying
	// NoiseTransport handshake prepends the preamble.
	ctx = noise.WithDialOverride(ctx, string(partitionID), keys)
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
