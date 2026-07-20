/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package manager

import (
	"github.com/ORBTR/aether"
)

// TenantSession wraps a aether.Connection and associates it with a tenant.
// Callers use TenantID() to determine which tenant the session belongs to,
// enabling transport-level tenant verification in the RPC executor.
type TenantSession struct {
	aether.Connection
	tenantID aether.ScopeID
}

// TenantID returns the tenant this session belongs to.
func (s *TenantSession) TenantID() aether.ScopeID {
	return s.tenantID
}

// Unwrap returns the underlying aether.Connection.
func (s *TenantSession) Unwrap() aether.Connection {
	return s.Connection
}

// WrapSession creates a TenantSession from an existing connection.
func WrapSession(conn aether.Connection, tenantID aether.ScopeID) *TenantSession {
	return &TenantSession{
		Connection: conn,
		tenantID:   tenantID,
	}
}
