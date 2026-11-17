/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"fmt"
	"sort"
)

// Catalogue is a registry's catalogue-only declarations: the operations of domains
// the endpoint only calls, listed so the endpoint's published peer record names
// their roles (Registry.Roles, read by node's Runtime.PublishRPCHandlersToLAD), and
// through that record node's role table and role-takeover coverage count, without
// the endpoint holding any of them as a handler.
//
// Nothing a Catalogue holds is a Handler of its registry: Get, All, ByRole,
// ByNamespace, MetadataFor, Dispatch and ExportToHandlerRegistry read the handlers
// alone, so a declaration here is never mounted by pkg/rpc/http or a route
// register, never dispatched in process and never proxied over the mesh. No route
// register or router takes a *Catalogue. That is the difference from RegisterProxy,
// whose stored handler every HTTP mount of the registry presents and forwards.
//
// A Catalogue is a view of one Registry (Registry.Catalogue) and shares its lock.
type Catalogue struct {
	reg *Registry
}

// Catalogue returns r's catalogue-only declarations.
func (r *Registry) Catalogue() *Catalogue {
	return &Catalogue{reg: r}
}

// Declare lists the operations of types, all of them or none. Only each
// declaration's Namespace, Domain and Operation are kept. It refuses a declaration
// whose FQN has an empty part, an FQN named twice in types or already listed, and an
// FQN the registry holds as a handler: an endpoint that serves an operation does
// not only call it, and an endpoint composing its catalogue after every
// registration of its own (node.orbtr.ai's registerRoutes) has its boot refused
// when one of its features registers an operation it lists as called elsewhere.
func (c *Catalogue) Declare(types []Handler) error {
	batch := make(map[string]string, len(types))
	for _, h := range types {
		if h.Namespace == "" || h.Domain == "" || h.Operation == "" {
			return fmt.Errorf("catalogue: declaration %q names no namespace, domain or operation", h.FQN())
		}
		fqn := h.FQN()
		if _, dup := batch[fqn]; dup {
			return fmt.Errorf("catalogue: %s declared twice in one batch", fqn)
		}
		batch[fqn] = h.Role()
	}
	r := c.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	for fqn := range batch {
		if _, held := r.handlers[fqn]; held {
			return fmt.Errorf("catalogue: %s is registered as a handler here, so this endpoint serves it", fqn)
		}
		if _, listed := r.listed[fqn]; listed {
			return fmt.Errorf("catalogue: %s already listed", fqn)
		}
	}
	if r.listed == nil {
		r.listed = make(map[string]string, len(batch))
	}
	for fqn, role := range batch {
		r.listed[fqn] = role
	}
	return nil
}

// Roles returns the roles of the listed operations, sorted, once each.
func (c *Catalogue) Roles() []string {
	c.reg.mu.RLock()
	defer c.reg.mu.RUnlock()
	return catalogueRoles(c.reg.listed)
}

// Lists reports whether fqn is a listed operation.
func (c *Catalogue) Lists(fqn string) bool {
	c.reg.mu.RLock()
	defer c.reg.mu.RUnlock()
	_, ok := c.reg.listed[fqn]
	return ok
}

// catalogueRoles is the sorted set of roles listed maps its FQNs to. The caller
// holds the registry's lock.
func catalogueRoles(listed map[string]string) []string {
	seen := make(map[string]struct{}, len(listed))
	roles := make([]string, 0, len(listed))
	for _, role := range listed {
		if _, ok := seen[role]; !ok {
			seen[role] = struct{}{}
			roles = append(roles, role)
		}
	}
	sort.Strings(roles)
	return roles
}
