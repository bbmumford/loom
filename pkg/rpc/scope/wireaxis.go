/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

// Axis names label the opaque identity axes loom carries on the wire. The wire
// itself is neutral: an AuthenticatedIdentity's Scopes marshal as a repeated
// pb.ScopeAxis{Name,Value} map (pkg/dispatch) and reconstruct generically from
// it (node/rpc.go bindAuthenticatedPrincipal); loom binds no axis to a named
// scalar proto field. loom assigns these names no meaning beyond "the value that
// travelled under this axis name" — a consumer's PresencePolicy and its
// ports.TransportAxisPin decide which axis pins against the transport-established
// tenant. The names below describe an axis by its TRANSPORT RELATIONSHIP, not by
// an identity level: they are the vocabulary a producer stamps and a consumer
// reads by, and both sides must agree on the string.
const (
	// AxisTransportPinned is the axis a consumer's ports.TransportAxisPin may
	// name as the one reconciled against the transport-established tenant on
	// receive (the cross-tenant forge gate). node/handlers validateScope
	// reconciles it against the transport binding; WithTransportTenant stamps it
	// from the server-resolved value.
	AxisTransportPinned = "platform_tenant"

	// AxisWithinTransport is the within-transport axis: it is deliberately NOT
	// the pinned axis, so a partition/owner derives from it within the pinned
	// tenant after the forge gate.
	AxisWithinTransport = "customer_org"
)
