/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package manager

import (
	"github.com/ORBTR/aether"
)

// PartitionSession wraps a aether.Connection and associates it with a tenant.
// Callers use PartitionID() to determine which tenant the session belongs to,
// enabling transport-level tenant verification in the RPC executor.
type PartitionSession struct {
	aether.Connection
	partitionID aether.ScopeID
}

// PartitionID returns the tenant this session belongs to.
func (s *PartitionSession) PartitionID() aether.ScopeID {
	return s.partitionID
}

// Unwrap returns the underlying aether.Connection.
func (s *PartitionSession) Unwrap() aether.Connection {
	return s.Connection
}

// WrapSession creates a PartitionSession from an existing connection.
func WrapSession(conn aether.Connection, partitionID aether.ScopeID) *PartitionSession {
	return &PartitionSession{
		Connection: conn,
		partitionID:   partitionID,
	}
}
