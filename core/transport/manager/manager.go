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
	ErrPartitionNotRegistered = errors.New("manager: partition transport not registered")
	ErrPartitionAlreadyExists = errors.New("manager: partition transport already registered")
	ErrManagerClosed          = errors.New("manager: transport manager is closed")
)

// TransportManager routes transport operations to per-partition NoiseTransport instances.
// It sits between Runtime and the transport layer, providing partition-aware Dial/Listen.
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

// Register adds a transport for a partition. Returns ErrPartitionAlreadyExists
// if the partition is already registered.
func (m *TransportManager) Register(partitionID aether.ScopeID, t aether.Transport) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.transports[partitionID]; exists {
		return ErrPartitionAlreadyExists
	}

	m.transports[partitionID] = t
	return nil
}

// Replace swaps the transport for an existing partition (used for key rotation).
// Returns ErrPartitionNotRegistered if the partition doesn't exist.
func (m *TransportManager) Replace(partitionID aether.ScopeID, t aether.Transport) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.transports[partitionID]; !exists {
		return ErrPartitionNotRegistered
	}

	m.transports[partitionID] = t
	return nil
}

// Remove deregisters a partition's aether. Does not close the aether.
// Returns ErrPartitionNotRegistered if the partition doesn't exist.
func (m *TransportManager) Remove(partitionID aether.ScopeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.transports[partitionID]; !exists {
		return ErrPartitionNotRegistered
	}

	delete(m.transports, partitionID)
	return nil
}

// Get returns the transport for a partition.
func (m *TransportManager) Get(partitionID aether.ScopeID) (aether.Transport, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrManagerClosed
	}
	t, ok := m.transports[partitionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPartitionNotRegistered, partitionID)
	}
	return t, nil
}

// Dial establishes a session to a remote node through the partition's aether.
// The returned session is wrapped as a PartitionSession carrying the partition ID.
// The partition ID is injected into the context so shared transports can use it
// for preamble construction during the handshake.
func (m *TransportManager) Dial(ctx context.Context, partitionID aether.ScopeID, target aether.Target) (*PartitionSession, error) {
	t, err := m.Get(partitionID)
	if err != nil {
		return nil, err
	}

	// Inject partition ID into context for shared transport preamble support.
	ctx = WithPartitionID(ctx, partitionID)

	session, err := t.Dial(ctx, target)
	if err != nil {
		return nil, err
	}

	return &PartitionSession{
		Connection:  session,
		partitionID: partitionID,
	}, nil
}

// partitionIDContextKey is the context key for passing partition ID through Dial.
type partitionIDContextKey struct{}

// WithPartitionID injects a partition ID into context.
func WithPartitionID(ctx context.Context, id aether.ScopeID) context.Context {
	return context.WithValue(ctx, partitionIDContextKey{}, id)
}

// PartitionIDFromContext extracts a partition ID from context.
func PartitionIDFromContext(ctx context.Context) (aether.ScopeID, bool) {
	id, ok := ctx.Value(partitionIDContextKey{}).(aether.ScopeID)
	return id, ok
}

// ListPartitions returns all registered partition IDs.
func (m *TransportManager) ListPartitions() []aether.ScopeID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]aether.ScopeID, 0, len(m.transports))
	for id := range m.transports {
		ids = append(ids, id)
	}
	return ids
}

// Count returns the number of registered partitions.
func (m *TransportManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.transports)
}

// ManagerStats is a point-in-time snapshot of TransportManager state.
// Surfaces per-partition transport type breakdown so operators can tell at
// a glance which partitions are on which transport family and whether any
// are running on a degraded path.
type ManagerStats struct {
	Closed         bool
	PartitionCount int
	Partitions     []PartitionStats // per-partition breakdown, sorted by partition ID
	ByTransport    map[string]int   // transport-type-name → partition count
}

// PartitionStats is per-partition observable state.
type PartitionStats struct {
	PartitionID   aether.ScopeID
	TransportType string // Go type name of the registered transport
	Protocol      string // aether.Protocol name if available
}

// Stats returns a full snapshot for diagnostics endpoints. Cheap — one
// RLock + O(partitions) string formatting.
func (m *TransportManager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := ManagerStats{
		Closed:         m.closed,
		PartitionCount: len(m.transports),
		Partitions:     make([]PartitionStats, 0, len(m.transports)),
		ByTransport:    make(map[string]int),
	}
	for tid, t := range m.transports {
		typeName := fmt.Sprintf("%T", t)
		proto := ""
		if prov, ok := t.(interface{ Protocol() aether.Protocol }); ok {
			proto = prov.Protocol().String()
		}
		out.Partitions = append(out.Partitions, PartitionStats{
			PartitionID:   tid,
			TransportType: typeName,
			Protocol:      proto,
		})
		out.ByTransport[typeName]++
	}
	return out
}

// RotateKey prepends a new key to the partition's key list, beginning dual-key overlap.
// The new key becomes the active key for outbound handshakes; old keys remain
// accepted for inbound handshakes until FinalizeRotation is called.
func (m *TransportManager) RotateKey(partitionID aether.ScopeID, newKey []byte) error {
	t, err := m.Get(partitionID)
	if err != nil {
		return err
	}

	keyCopy := append([]byte(nil), newKey...)

	switch tx := t.(type) {
	case *SharedTransport:
		currentKeys, err := tx.ResolveKeys(string(partitionID))
		if err != nil {
			return fmt.Errorf("resolve current keys: %w", err)
		}
		newKeys := append([][]byte{keyCopy}, currentKeys...)
		return tx.UpdatePartitionKeys(partitionID, newKeys)

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
func (m *TransportManager) FinalizeRotation(partitionID aether.ScopeID) error {
	t, err := m.Get(partitionID)
	if err != nil {
		return err
	}

	switch tx := t.(type) {
	case *SharedTransport:
		currentKeys, err := tx.ResolveKeys(string(partitionID))
		if err != nil {
			return fmt.Errorf("resolve current keys: %w", err)
		}
		if len(currentKeys) <= 1 {
			return nil // Already finalized
		}
		return tx.UpdatePartitionKeys(partitionID, currentKeys[:1])

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
			firstErr = fmt.Errorf("close transport for partition %s: %w", id, err)
		}
	}
	m.transports = nil
	return firstErr
}
