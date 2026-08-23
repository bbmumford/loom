/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import (
	"context"
	"testing"
)

func TestCheckPresence(t *testing.T) {
	full := AuthenticatedIdentity{
		PlatformTenantID: "orbtr",
		OrganizationID:   "org-1",
		UserID:           "user-1",
	}
	tests := []struct {
		name       string
		declared   TenantScope
		identity   AuthenticatedIdentity
		isPlatform bool
		want       PresenceVerdict
	}{
		{name: "none anonymous", declared: None, want: PresenceSatisfied},
		{name: "platform missing", declared: Platform, want: PresencePlatformRequired},
		{name: "platform not allowlisted", declared: Platform, identity: full, want: PresencePlatformRequired},
		{name: "platform exact", declared: Platform, identity: full, isPlatform: true, want: PresenceSatisfied},
		{name: "tenant missing", declared: Tenant, want: PresenceTenantRequired},
		{name: "tenant exact", declared: Tenant, identity: full, want: PresenceSatisfied},
		{name: "org missing tenant", declared: Org, want: PresenceTenantRequired},
		{
			name:     "org platform only",
			declared: Org,
			identity: AuthenticatedIdentity{PlatformTenantID: "orbtr"},
			want:     PresenceOrganizationRequired,
		},
		{name: "org exact", declared: Org, identity: full, want: PresenceSatisfied},
		{name: "user missing tenant", declared: User, want: PresenceTenantRequired},
		{
			name:     "user platform only",
			declared: User,
			identity: AuthenticatedIdentity{PlatformTenantID: "orbtr"},
			want:     PresenceUserRequired,
		},
		{name: "user exact", declared: User, identity: full, want: PresenceSatisfied},
		{name: "profile missing tenant", declared: Profile, want: PresenceTenantRequired},
		{name: "profile exact", declared: Profile, identity: full, want: PresenceSatisfied},
		{name: "unknown", declared: Unknown, identity: full, isPlatform: true, want: PresenceUnknownScope},
		{name: "unset", declared: Unset, identity: full, isPlatform: true, want: PresenceUnknownScope},
		{name: "garbage", declared: TenantScope("garbage"), identity: full, isPlatform: true, want: PresenceUnknownScope},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CheckPresence(test.declared, test.identity, test.isPlatform); got != test.want {
				t.Fatalf("CheckPresence() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPlatformTenantsFailClosedAndMatchExactly(t *testing.T) {
	tests := []struct {
		name     string
		allowed  PlatformTenants
		tenantID string
		want     bool
	}{
		{name: "unset", tenantID: "hstles"},
		{name: "empty", allowed: NewPlatformTenants(), tenantID: "hstles"},
		{name: "empty identity", allowed: NewPlatformTenants("hstles")},
		{name: "foreign", allowed: NewPlatformTenants("hstles"), tenantID: "hstles-dev"},
		{name: "exact", allowed: NewPlatformTenants("hstles"), tenantID: "hstles", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.allowed.IsPlatform(test.tenantID); got != test.want {
				t.Fatalf("IsPlatform(%q) = %v, want %v", test.tenantID, got, test.want)
			}
		})
	}
}

func TestWithAuthenticatedPlatformTenantReplacesWholeSnapshot(t *testing.T) {
	ctx := WithAuthenticatedIdentity(context.Background(), AuthenticatedIdentity{
		PlatformTenantID: "old-platform",
		OrganizationID:   "old-org",
		UserID:           "old-user",
	})

	got := AuthenticatedIdentityFromContext(
		WithAuthenticatedPlatformTenant(ctx, "new-platform"),
	)
	want := (AuthenticatedIdentity{PlatformTenantID: "new-platform"})
	if got != want {
		t.Fatalf("platform rebind retained stale identity fields: got %+v want %+v", got, want)
	}
}
