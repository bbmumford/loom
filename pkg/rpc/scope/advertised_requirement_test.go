package scope

import "testing"

// Two declarations that admit the same callers read the same, and two that do not
// read differently, because this string is what peers compare across adverts.
func TestAdvertisedRequirementReadsWhatEnforcementReads(t *testing.T) {
	capability, open, nexus, unset := Capability("orbtr:audit.write"), Open(), Nexus(), Requirement{}
	for _, c := range []struct {
		req  *Requirement
		tag  ScopeTag
		want string
	}{
		{&capability, ScopeNone, "capability:orbtr:audit.write"},
		{&open, ScopePartition, "open"},
		{&nexus, ScopeNone, "nexus"},
		{&unset, ScopeNone, "unset"},
		{nil, ScopeNone, "open"},
		{nil, ScopePartition, "scope:partition"},
		{nil, ScopeUnset, "scope:"},
	} {
		if got := AdvertisedRequirement(c.req, c.tag); got != c.want {
			t.Errorf("AdvertisedRequirement(%+v, %q) = %q, want %q", c.req, c.tag, got, c.want)
		}
	}
}
