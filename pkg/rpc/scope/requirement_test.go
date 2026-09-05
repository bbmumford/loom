package scope

import "testing"

// capSet admits exactly the capabilities it holds (a fake authorizer; it ignores
// the principal's own grants and answers from its held set).
type capSet map[string]bool

func (c capSet) HasCapability(_ AuthenticatedPrincipal, cap string) bool { return c[cap] }

func resolved(partition string) AuthenticatedPrincipal {
	p, _ := NewAuthenticatedPrincipal(AuthenticatedIdentity{}.WithPartition(partition), nil)
	return p
}

func TestRequirement_Open(t *testing.T) {
	if ok, _ := NewPolicy(nil).Authorize(Open(), AuthenticatedPrincipal{}); !ok {
		t.Fatal("open op should admit an unresolved identity")
	}
}

func TestRequirement_Capability(t *testing.T) {
	p := NewPolicy(capSet{"orbtr.ai:platform.admin": true})
	id := resolved("p1")
	if ok, _ := p.Authorize(Capability("orbtr.ai:platform.admin"), id); !ok {
		t.Fatal("held capability should admit")
	}
	if ok, r := p.Authorize(Capability("orbtr.ai:billing.write"), id); ok {
		t.Fatal("unheld capability should deny")
	} else if r == "" {
		t.Fatal("deny should carry a reason")
	}
}

func TestRequirement_CapabilityFailsClosed(t *testing.T) {
	if ok, _ := NewPolicy(capSet{}).Authorize(Requirement{Kind: ArmCapability}, resolved("p1")); ok {
		t.Fatal("capability-arm op with no declared cap should deny")
	}
	if ok, _ := NewPolicy(nil).Authorize(Capability("orbtr.ai:platform.admin"), resolved("p1")); ok {
		t.Fatal("capability-arm op with nil authorizer should deny")
	}
}

// The nexus arm gates on a census-resolved identity (opaque partition present) and
// nothing more — no level. Resolved passes; unresolved fails closed.
func TestRequirement_NexusResolvedOnly(t *testing.T) {
	p := NewPolicy(nil)
	if ok, _ := p.Authorize(Nexus(), resolved("p1")); !ok {
		t.Fatal("a resolved partition should pass the nexus authn gate")
	}
	if ok, r := p.Authorize(Nexus(), AuthenticatedPrincipal{}); ok {
		t.Fatal("an unresolved identity should fail closed")
	} else if r == "" {
		t.Fatal("deny should carry a reason")
	}
}

// Nexus is AUTHN, not authority: every resolved identity passes; the policy never
// decides which one owns the target — that is the handler's nexus check, and is why
// there is no subject/partition level here.
func TestRequirement_NexusIsAuthnNotAuthority(t *testing.T) {
	p := NewPolicy(nil)
	if ok, _ := p.Authorize(Nexus(), resolved("owner-scope")); !ok {
		t.Fatal("owner's partition passes authn")
	}
	if ok, _ := p.Authorize(Nexus(), resolved("stranger-scope")); !ok {
		t.Fatal("any resolved identity passes authn; ownership is the handler's nexus check")
	}
}

func TestRequirement_UnknownArmFailsClosed(t *testing.T) {
	if ok, _ := NewPolicy(capSet{}).Authorize(Requirement{Kind: ArmKind(99)}, resolved("p1")); ok {
		t.Fatal("unknown arm should fail closed")
	}
}
