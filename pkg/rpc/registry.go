package rpc

import (
	"fmt"
	"sort"
	"sync"
)

// Registry holds all registered handlers, indexed by FQN.
// Thread-safe for concurrent registration during init.
type Registry struct {
	mu        sync.RWMutex
	handlers  map[string]*Handler   // FQN → Handler
	byRole    map[string][]*Handler // "namespace.domain" → handlers
	platforms PlatformTenants       // set of tenantIds that satisfy ScopePlatform
}

// NewRegistry creates an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]*Handler),
		byRole:   make(map[string][]*Handler),
	}
}

// SetPlatformTenants configures the set of tenantIds whose calls satisfy
// ScopePlatform handlers. Must be called before Dispatch / HandleRPC starts
// accepting traffic. Pass nil (the zero value) to keep platform checks
// failing closed — the safer default for any registry where the platform
// set is genuinely unknown.
func (r *Registry) SetPlatformTenants(t PlatformTenants) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.platforms = t
}

// PlatformTenants returns the configured platform set (or nil).
// Read-only — callers must not mutate the returned map.
func (r *Registry) PlatformTenants() PlatformTenants {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.platforms
}

// Register adds a handler to the registry.
//
// Mutability defence: the Middleware and Tags slice headers on h alias the
// caller's backing arrays. We copy them so a caller that later mutates
// their own slice (e.g. `mw = append(mw, extra)`) cannot retroactively
// modify what the registry hands out via Get/All/ByRole.
func (r *Registry) Register(h Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	fqn := h.FQN()
	if _, exists := r.handlers[fqn]; exists {
		return fmt.Errorf("handler %s already registered", fqn)
	}

	stored := h
	if len(h.Middleware) > 0 {
		stored.Middleware = append([]Middleware(nil), h.Middleware...)
	}
	if len(h.Tags) > 0 {
		stored.Tags = append([]string(nil), h.Tags...)
	}
	r.handlers[fqn] = &stored
	role := h.Role()
	r.byRole[role] = append(r.byRole[role], &stored)
	return nil
}

// RegisterAll registers multiple handlers. Stops on first error.
func (r *Registry) RegisterAll(handlers []Handler) error {
	for _, h := range handlers {
		if err := r.Register(h); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a handler by FQN.
func (r *Registry) Get(fqn string) (*Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[fqn]
	return h, ok
}

// All returns all registered handlers sorted by FQN.
func (r *Registry) All() []*Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*Handler, 0, len(r.handlers))
	for _, h := range r.handlers {
		result = append(result, h)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].FQN() < result[j].FQN() })
	return result
}

// Roles returns all unique namespace.domain role strings.
func (r *Registry) Roles() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	roles := make([]string, 0, len(r.byRole))
	for role := range r.byRole {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	return roles
}

// ByRole returns a copy of all handlers for a given namespace.domain.
func (r *Registry) ByRole(role string) []*Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src := r.byRole[role]
	if src == nil {
		return nil
	}
	out := make([]*Handler, len(src))
	copy(out, src)
	return out
}

// ByNamespace returns all handlers in a given namespace.
func (r *Registry) ByNamespace(ns string) []*Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []*Handler
	prefix := ns + "."
	for fqn, h := range r.handlers {
		if len(fqn) > len(prefix) && fqn[:len(prefix)] == prefix {
			result = append(result, h)
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
// Use this on gateway endpoints (app.orbtr.io, devices.orbtr.io) to register
// domain handlers that run on node endpoints:
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
// the HTTP bridge / per-route mounts.
func (r *Registry) RegisterProxy(handlers []Handler) error {
	for _, h := range handlers {
		h.Func = nil
		h.Middleware = nil
		if err := r.Register(h); err != nil {
			return err
		}
	}
	return nil
}

// MetadataFor returns the stored Handler for fqn with Func stripped — i.e. the
// same shape a domain Types() function returns today. Used by gateway / HTTP
// bridge code paths that need scope, request/response prototypes, tags, and
// op-class metadata but must NOT carry the executable Func (proxy registration
// strips it; this accessor makes that contract explicit without round-tripping
// through Get + manual stripping).
//
// The returned Handler is a defensive copy: Middleware and Tags slices share
// no backing array with the registry's stored copy, so callers may freely
// mutate the result without corrupting registry state.
func (r *Registry) MetadataFor(fqn string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[fqn]
	if !ok {
		return Handler{}, false
	}
	out := *h
	out.Func = nil
	if len(h.Middleware) > 0 {
		out.Middleware = append([]Middleware(nil), h.Middleware...)
	}
	if len(h.Tags) > 0 {
		out.Tags = append([]string(nil), h.Tags...)
	}
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
		h.Func = nil
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
