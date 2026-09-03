package scope

import "testing"

func TestNewScopeIdentity_AxesAndPartition(t *testing.T) {
	id := NewScopeIdentity("pt1", "org1", "usr1")
	if got := id.Scope(AxisTransportPinned); got != "pt1" {
		t.Fatalf("transport-pinned axis: got %q want pt1", got)
	}
	if got := id.Scope(AxisWithinTransport); got != "org1" {
		t.Fatalf("within-transport axis: got %q want org1", got)
	}
	if id.Subject != "usr1" {
		t.Fatalf("subject: got %q want usr1", id.Subject)
	}
	if id.Partition() != "org1" {
		t.Fatalf("partition: got %q want org1 (within-transport axis)", id.Partition())
	}
	// An omitted axis stays absent (empty string == omitted through Scope).
	bare := NewScopeIdentity("", "", "")
	if bare.Scope(AxisTransportPinned) != "" || bare.Partition() != "" {
		t.Fatal("all-empty identity must carry no axes and no partition")
	}
}

func TestNewScopePresencePolicy_Tiers(t *testing.T) {
	p := NewScopePresencePolicy("orbtr.ai")
	platform := NewScopeIdentity("orbtr.ai", "", "")
	tenant := NewScopeIdentity("t1", "", "")
	full := NewScopeIdentity("t1", "org1", "usr1")
	none := NewScopeIdentity("", "", "")

	cases := []struct {
		name     string
		declared ScopeTag
		id       AuthenticatedIdentity
		want     bool
	}{
		{"none admits anyone", ScopeNone, none, true},
		{"platform needs allowlisted pinned", ScopePlatform, platform, true},
		{"platform denies non-allowlisted", ScopePlatform, tenant, false},
		{"platform denies empty", ScopePlatform, none, false},
		{"tenant needs pinned", ScopeTenant, tenant, true},
		{"tenant denies empty", ScopeTenant, none, false},
		{"partition (neutral) needs pinned", ScopePartition, tenant, true},
		{"org needs pinned+within", ScopeOrg, full, true},
		{"org denies pinned-only", ScopeOrg, tenant, false},
		{"user needs pinned+subject", ScopeUser, full, true},
		{"user denies no-subject", ScopeUser, tenant, false},
		{"profile needs pinned", ScopeProfile, tenant, true},
		{"unknown fails closed", ScopeUnknown, full, false},
	}
	for _, c := range cases {
		if ok, reason := p.CheckPresence(c.declared, c.id); ok != c.want {
			t.Errorf("%s: got %v (%q) want %v", c.name, ok, reason, c.want)
		}
	}
}

func TestNewScopePresencePolicy_EmptyAllowlistFailsClosedPlatform(t *testing.T) {
	p := NewScopePresencePolicy() // no platform partitions
	if ok, _ := p.CheckPresence(ScopePlatform, NewScopeIdentity("anything", "", "")); ok {
		t.Fatal("empty allowlist must never satisfy ScopePlatform")
	}
	// Non-platform tiers still turn on axis presence.
	if ok, _ := p.CheckPresence(ScopeTenant, NewScopeIdentity("t1", "", "")); !ok {
		t.Fatal("tenant tier turns on pinned-axis presence, independent of the platform allowlist")
	}
}
