/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

// Axis names map loom's opaque identity axes onto the deployed
// pb.AuthenticatedPrincipal wire fields WITHOUT changing the proto. The send
// path (pkg/dispatch) reads AuthenticatedIdentity.Scope(AxisPlatformTenant)
// into pb.AuthenticatedPrincipal.PlatformTenantId and
// .Scope(AxisCustomerOrg) into .CustomerOrgId; the receive path
// (node/rpc.go bindAuthenticatedPrincipal) reconstructs the identity by
// appending these named axes from those same wire fields.
//
// They are the ONLY axis names loom itself binds to a wire field. A consumer's
// PresencePolicy may key on them, but loom assigns them no meaning beyond
// "the value that travelled in this proto field".
const (
	// AxisPlatformTenant is the identity axis carried by
	// pb.AuthenticatedPrincipal.PlatformTenantId. The receive side reconciles
	// it against the tenant-bound transport before trusting the principal.
	AxisPlatformTenant = "platform_tenant"

	// AxisCustomerOrg is the identity axis carried by
	// pb.AuthenticatedPrincipal.CustomerOrgId.
	AxisCustomerOrg = "customer_org"
)
