package scope

// Requirement is an operation's authorization requirement, declared at handler
// registration in place of a ScopeTag. Authority is TWO arms and NO identity level:
// a warrant CAPABILITY, or a census-resolved identity whose authorization is a
// nexus-graph relationship the handler resolves. "user", "tenant" and "org" are
// not levels here — they are opaque entities the handler's nexus check reasons
// about; the seam never names or ranks them. Kind selects the arm; Cap is read only
// for ArmCapability.
type Requirement struct {
	Kind ArmKind
	Cap  string
}

// ArmKind is how an operation is authorized. There is no partition/user/org level:
// authority is a capability, or a nexus relationship the handler resolves.
type ArmKind int

const (
	// ArmOpen — no authorization (liveness, public status). Always admits. It is
	// the zero value, so a Requirement left unset denies nothing on its own — the
	// enforcement seam treats an explicitly-declared arm as the gate.
	ArmOpen ArmKind = iota
	// ArmCapability — a warrant capability admits it. The seam checks the identity
	// holds it (an operator-only op, or a role-gated op that opted into a capability).
	ArmCapability
	// ArmNexus — a census-resolved identity is required (the seam's authn gate: the
	// opaque partition is present, fail-closed if not); authorization is the
	// handler's nexus-graph resolution, not a capability and not a level.
	ArmNexus
)

// Open is the requirement for an unauthenticated op.
func Open() Requirement { return Requirement{Kind: ArmOpen} }

// Capability is the requirement for a capability-arm op admitted by cap
// (a warrant capability, e.g. "orbtr.ai:platform.admin").
func Capability(cap string) Requirement { return Requirement{Kind: ArmCapability, Cap: cap} }

// Nexus is the requirement for a nexus-arm op: a census-resolved identity, with the
// handler owning authorization through its nexus-graph resolution.
func Nexus() Requirement { return Requirement{Kind: ArmNexus} }

// Authorizer is the capability-arm seam: reports whether the sealed principal holds
// capability cap. It receives the AuthenticatedPrincipal — the identity coupled with
// its granted scope strings (principal.Scopes()) — because caps are resolved over the
// caller's HELD grants, which live on the principal, not the bare identity. A
// warrant-backed authorizer (a consumer's warrant.Authorizer adapter that runs its
// cap grammar over principal.Scopes()) satisfies it; the composition root injects one.
// loom defines only the interface — it never imports warrant. A nil Authorizer makes
// every capability-arm op fail closed.
type Authorizer interface {
	HasCapability(principal AuthenticatedPrincipal, cap string) bool
}

// Policy enforces the seam-owned part of each arm over a sealed AuthenticatedPrincipal.
// It holds only the injected capability authorizer; the nexus arm needs no
// dependency (an authn check on the principal's opaque partition).
type Policy struct {
	az Authorizer
}

// NewPolicy builds a policy over a capability authorizer. az may be nil, in which
// case every capability-arm op fails closed (never silently admits).
func NewPolicy(az Authorizer) *Policy { return &Policy{az: az} }

// Authorize reports whether the sealed principal may invoke an op with the given
// requirement, and a non-authoritative reason when it may not. FAIL-CLOSED throughout:
// an unknown arm, a capability-arm op with no declared capability or no authorizer, a
// capability the principal's grants do not confer, or a nexus-arm identity with no
// resolved partition all DENY. The reason is a diagnostic only; callers must not parse it.
func (p *Policy) Authorize(req Requirement, principal AuthenticatedPrincipal) (allowed bool, reason string) {
	switch req.Kind {
	case ArmOpen:
		return true, ""
	case ArmCapability:
		if req.Cap == "" {
			return false, "capability-arm op declared no capability"
		}
		if p.az == nil {
			return false, "no capability authorizer configured"
		}
		if p.az.HasCapability(principal, req.Cap) {
			return true, ""
		}
		return false, "principal grants do not confer the required capability"
	case ArmNexus:
		if principal.Identity().Partition() == "" {
			return false, "requires a census-resolved identity"
		}
		// Census-resolved. Authorization is the handler's nexus-graph resolution.
		return true, ""
	default:
		return false, "unknown authorization arm"
	}
}
