package rpc

import (
	"fmt"
	"sort"
	"sync"

	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// Registry holds all registered handlers, indexed by FQN.
// Thread-safe for concurrent registration during init.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]*Handler   // FQN → Handler
	byRole   map[string][]*Handler // "namespace.domain" → handlers
	policy   scope.PresencePolicy  // consumer-supplied scope evaluator (nil fails closed)
	authz    *scope.Policy         // two-arm op authorizer; EnforceScope consults it for a handler that declares a Requirement (nil → the Scope/PresencePolicy tier path)
	refusals scope.RefusalRecorder // the server's refusal recorder; Enforce calls it for every refusal (nil returns the refusal unrecorded)
	listed   map[string]string     // catalogue-only declarations (Catalogue): FQN → role; Roles reads it, no handler lookup does

	// parent is the registry Derive built this one from: while a policy or the
	// recorder is not set here (policySet, authzSet, refusalsSet), its accessor
	// reads the parent's.
	parent                           *Registry
	policySet, authzSet, refusalsSet bool
}

// NewRegistry creates an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]*Handler),
		byRole:   make(map[string][]*Handler),
	}
}

// Derive returns an empty registry that judges calls under r's presence policy,
// authorization policy and refusal recorder. Each is read from r at every call
// until it is set on the derived registry (a Set call, even with nil, ends the
// inheritance of that one alone), so a recorder a server registers on r after
// deriving still records the derived registry's refusals. A server that serves
// part of its routes from a registry beside its main one (a proxied or forwarded
// surface) derives it, so none of its refusals goes unrecorded for want of a
// copied recorder.
func (r *Registry) Derive() *Registry {
	d := NewRegistry()
	d.parent = r
	return d
}

// SetPresencePolicy configures the consumer-supplied evaluator that decides,
// for every declared scope other than ScopeNone, whether the server-established
// identity satisfies it. Must be called before Dispatch / HandleRPC starts
// accepting traffic. Pass nil (the zero value) to keep scope enforcement
// failing closed — the safer default for any registry that has not adopted a
// policy: EnforceScope denies every non-None handler when the policy is nil.
func (r *Registry) SetPresencePolicy(p scope.PresencePolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policy, r.policySet = p, true
}

// PresencePolicy returns the configured scope evaluator, the parent's for a
// derived registry on which none was set (or nil). A nil policy fails closed at
// EnforceScope for every non-None handler.
func (r *Registry) PresencePolicy() scope.PresencePolicy {
	r.mu.RLock()
	p, set, parent := r.policy, r.policySet, r.parent
	r.mu.RUnlock()
	if !set && parent != nil {
		return parent.PresencePolicy()
	}
	return p
}

// SetAuthzPolicy installs the two-arm op authorizer (capability | nexus) that
// EnforceScope consults for any handler declaring a scope.Requirement. Until a
// composition root installs one, every Requirement-bearing handler fails CLOSED
// at EnforceScope — the same fail-closed posture as a nil PresencePolicy — so a
// handler cannot opt into two-arm authorization and then silently admit.
func (r *Registry) SetAuthzPolicy(p *scope.Policy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.authz, r.authzSet = p, true
}

// AuthzPolicy returns the configured two-arm op authorizer, the parent's for a
// derived registry on which none was set (or nil). A nil authz fails closed at
// EnforceScope for every handler that declares a Requirement.
func (r *Registry) AuthzPolicy() *scope.Policy {
	r.mu.RLock()
	p, set, parent := r.authz, r.authzSet, r.parent
	r.mu.RUnlock()
	if !set && parent != nil {
		return parent.AuthzPolicy()
	}
	return p
}

// SetRefusalRecorder registers the recorder Enforce calls for every call it
// refuses, before the refusal returns: the HTTP bridge's local, proxied and
// streamed calls and Dispatch all pass through Enforce. library endpoint
// registers the audit host's recorder here and on the mesh HandlerRegistry.
// Must be called before the registry accepts traffic.
func (r *Registry) SetRefusalRecorder(rec scope.RefusalRecorder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refusals, r.refusalsSet = rec, true
}

// RefusalRecorder returns the registered refusal recorder, the parent's for a
// derived registry on which none was registered (or nil).
func (r *Registry) RefusalRecorder() scope.RefusalRecorder {
	r.mu.RLock()
	rec, set, parent := r.refusals, r.refusalsSet, r.parent
	r.mu.RUnlock()
	if !set && parent != nil {
		return parent.RefusalRecorder()
	}
	return rec
}

// owned returns a copy of h that shares no memory a caller can write with h:
// the Middleware and Tags slices, the Requirement and the WebhookSignature
// configuration are copied. The Request and Response prototypes are typed nil
// pointers and the functions are values, so they are shared as they are.
// prepareStored stores such a copy, and every accessor that hands a handler
// out (Get, All, ByRole, ByNamespace, MetadataFor) and the mesh export
// (ExportToHandlerRegistry) take one from the stored handler, so the handler
// the HTTP bridge, the mesh dispatch and the publication read changes only by
// registration.
func owned(h *Handler) *Handler {
	c := *h
	if len(h.Middleware) > 0 {
		c.Middleware = append([]Middleware(nil), h.Middleware...)
	}
	if len(h.Tags) > 0 {
		c.Tags = append([]string(nil), h.Tags...)
	}
	if h.Requirement != nil {
		req := *h.Requirement
		c.Requirement = &req
	}
	if h.WebhookSignature != nil {
		sig := *h.WebhookSignature
		c.WebhookSignature = &sig
	}
	return &c
}

// ownedAll is owned over a list, in its order.
func ownedAll(list []*Handler) []*Handler {
	out := make([]*Handler, len(list))
	for i, h := range list {
		out[i] = owned(h)
	}
	return out
}

// prepareStored validates the handler's scope tag and Requirement arm, has the
// installed DeclarationReader read the owned copy of h, and returns its FQN, role,
// and that copy as the stored Handler. It takes no lock and touches no map, so it
// is the shared lock-free half of Register and the atomic batch core (RegisterAll
// and RegisterProxy): scope validation, the copy and the contract's reading live
// here once. Collision checks and the store happen under the caller's lock.
func (r *Registry) prepareStored(h Handler) (fqn, role string, stored *Handler, err error) {
	if !scope.IsDeclared(h.Scope) {
		return "", "", nil, fmt.Errorf("handler %s has a non-declarable scope %q: declare a concrete scope tag (the ScopeUnset zero value and the ScopeUnknown proto-decay sentinel are refused)", h.FQN(), h.Scope)
	}
	if h.Requirement != nil && h.Requirement.Kind == scope.ArmUnset {
		return "", "", nil, fmt.Errorf("handler %s declares a Requirement with no authorization arm: build it with scope.Open, scope.Capability or scope.Nexus", h.FQN())
	}
	stored = owned(&h)
	if err := readDeclaration(stored); err != nil {
		return "", "", nil, err
	}
	return stored.FQN(), stored.Role(), stored, nil
}

// Register adds a handler to the registry.
//
// The registry stores an owned copy of h (owned), so a caller that later
// changes its own Middleware or Tags slice, its Requirement or its
// WebhookSignature configuration does not change what the registry serves.
func (r *Registry) Register(h Handler) error {
	fqn, role, stored, err := r.prepareStored(h)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[fqn]; exists {
		return fmt.Errorf("handler %s already registered", fqn)
	}
	r.handlers[fqn] = stored
	r.byRole[role] = append(r.byRole[role], stored)
	return nil
}

// registerAllAtomic is the lock-scoped prepare/preflight/commit core for
// RegisterAll and RegisterProxy. It prepares every handler lock-free — proxy
// stripping when proxy is set, scope validation, defensive copies, and
// within-batch duplicate detection in input order — then under one write lock
// preflights every FQN against the live handlers map and commits handlers+byRole
// only after all pass. Any failure returns the offending error with the registry
// byte-unchanged, so Get/All/ByRole observe either the old state or the full new
// batch and never a partial prefix. It does not call Register (r.mu is not
// reentrant); the shared prepareStored keeps the two paths in one grammar.
func (r *Registry) registerAllAtomic(handlers []Handler, proxy bool) error {
	type prep struct {
		fqn, role string
		stored    *Handler
	}
	preps := make([]prep, 0, len(handlers))
	seen := make(map[string]struct{}, len(handlers))
	for _, h := range handlers {
		if proxy {
			h.Func, h.Stream = nil, nil
			h.Middleware = nil
		}
		fqn, role, stored, err := r.prepareStored(h)
		if err != nil {
			return err
		}
		if _, dup := seen[fqn]; dup {
			return fmt.Errorf("handler %s duplicated within the same registration batch", fqn)
		}
		seen[fqn] = struct{}{}
		preps = append(preps, prep{fqn: fqn, role: role, stored: stored})
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, p := range preps {
		if _, exists := r.handlers[p.fqn]; exists {
			return fmt.Errorf("handler %s already registered", p.fqn)
		}
	}
	for _, p := range preps {
		r.handlers[p.fqn] = p.stored
		r.byRole[p.role] = append(r.byRole[p.role], p.stored)
	}
	return nil
}

// RegisterAll registers multiple handlers atomically: either every handler is
// stored or none is, so a failing member never leaves a partial prefix behind.
func (r *Registry) RegisterAll(handlers []Handler) error {
	return r.registerAllAtomic(handlers, false)
}

// Get retrieves a copy of the handler registered under fqn (owned): changing
// it changes nothing the registry serves.
func (r *Registry) Get(fqn string) (*Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[fqn]
	if !ok {
		return nil, false
	}
	return owned(h), true
}

// All returns a copy of every registered handler (owned), sorted by FQN.
func (r *Registry) All() []*Handler {
	r.mu.RLock()
	result := make([]*Handler, 0, len(r.handlers))
	for _, h := range r.handlers {
		result = append(result, owned(h))
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].FQN() < result[j].FQN() })
	return result
}

// Roles returns all unique namespace.domain role strings: the role of every
// handler and of every catalogue-only declaration (Catalogue), sorted. It is what
// a published registry advertises (node's Runtime.PublishRPCHandlersToLAD), so a
// domain an endpoint only calls stays on its role list without a handler here.
func (r *Registry) Roles() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	roles := make([]string, 0, len(r.byRole))
	for role := range r.byRole {
		roles = append(roles, role)
	}
	for _, role := range catalogueRoles(r.listed) {
		if _, held := r.byRole[role]; !held {
			roles = append(roles, role)
		}
	}
	sort.Strings(roles)
	return roles
}

// ByRole returns a copy of every handler of a given namespace.domain (owned),
// in registration order.
func (r *Registry) ByRole(role string) []*Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src := r.byRole[role]
	if src == nil {
		return nil
	}
	return ownedAll(src)
}

// ByNamespace returns a copy of every handler in a given namespace (owned).
func (r *Registry) ByNamespace(ns string) []*Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*Handler
	prefix := ns + "."
	for fqn, h := range r.handlers {
		if len(fqn) > len(prefix) && fqn[:len(prefix)] == prefix {
			result = append(result, owned(h))
		}
	}
	return result
}

// Count returns the total number of registered handlers.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// RegisterProxy registers handlers with type metadata only (nil Func).
// These serve as proxy endpoints — the HTTP bridge dispatches them over
// mesh via CallRaw instead of executing locally.
//
// Use this on a gateway endpoint to register domain handlers that run on
// node endpoints:
//
//	proxyReg := rpc.NewRegistry()
//	proxyReg.RegisterProxy(device.Types())
//	proxyReg.RegisterProxy(policy.Types())
//	rpchttp.RegisterAll(proxyReg, router)
//
// Middleware on the source Handler is INTENDED for the implementing node
// (where Func actually runs); the gateway must not re-enforce it or we'd
// double-charge the same auth checks. RegisterProxy strips Middleware
// before storing — gateways apply their own gateway-tier middleware via
// the HTTP bridge / per-route mounts. The installed DeclarationReader then
// reads the stripped copy like any other registration, so what the
// operation's contract declares for its HTTP mounts (its exposure, a limit
// it writes as Middleware) holds on the gateway's mount, which is where an
// HTTP request reaches it: the mesh hop to the implementing node runs no
// Middleware.
func (r *Registry) RegisterProxy(handlers []Handler) error {
	return r.registerAllAtomic(handlers, true)
}

// MetadataFor returns the stored Handler for fqn with Func stripped — i.e. the
// same shape a domain Types() function returns today. Used by gateway / HTTP
// bridge code paths that need scope, request/response prototypes, tags, and
// op-class metadata but must NOT carry the executable Func (proxy registration
// strips it; this accessor makes that contract explicit without round-tripping
// through Get + manual stripping).
//
// The returned Handler is an owned copy (owned), so callers may freely
// mutate the result without corrupting registry state.
func (r *Registry) MetadataFor(fqn string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[fqn]
	if !ok {
		return Handler{}, false
	}
	out := *owned(h)
	out.Func, out.Stream = nil, nil
	return out, true
}

// StripFuncs returns a derived view of handlers with Func cleared on every
// element — the canonical helper for building a domain's Types() list from
// its single authoritative Register() handler slice. Other fields (Scope,
// Request, Response, Tags, OpClass, Middleware) are preserved as-is.
//
// Pattern (single source of truth — no duplicated handler list):
//
//	var handlers = []rpc.Handler{ ... canonical list with Func wired ... }
//	func Register(reg *rpc.Registry) error { return reg.RegisterAll(handlers) }
//	func Types() []rpc.Handler             { return rpc.StripFuncs(handlers) }
//
// Slice headers for Middleware/Tags are copied so a mutation on the returned
// view's element does not silently rewrite the source canonical list.
func StripFuncs(handlers []Handler) []Handler {
	if len(handlers) == 0 {
		return nil
	}
	out := make([]Handler, len(handlers))
	for i, h := range handlers {
		h.Func, h.Stream = nil, nil
		if len(h.Middleware) > 0 {
			h.Middleware = append([]Middleware(nil), h.Middleware...)
		}
		if len(h.Tags) > 0 {
			h.Tags = append([]string(nil), h.Tags...)
		}
		out[i] = h
	}
	return out
}
