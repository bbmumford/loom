/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sort"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/pkg/rpc/effect"
	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// catalogueEntry is one operation a node's peer record says it serves: the FQN
// RpcHandlers lists, the effect class effectTags carries for it and the
// requirement requirementTag carries (scope.AdvertisedRequirement of what it
// declares). PeerPublisher holds a snapshot of them, sorted by FQN, which only
// commit replaces.
type catalogueEntry struct {
	fqn         string
	role        string
	effect      effect.Class
	requirement string
}

// catalogueOf is what the node's handler registry dispatches, as the record
// lists it: every handler of HandlerRegistry.Catalogue (loom's own mesh
// handlers left out), sorted by FQN. A handler's advertised requirement is
// the one it declares for its advert (handlers.AdvertDeclarer), else the one
// its gate judges (handlers.RequirementDeclarer), with its scope tag.
// Runtime.PublishRPCHandlersToLAD commits it.
func catalogueOf(reg *handlers.HandlerRegistry) []catalogueEntry {
	if reg == nil {
		return nil
	}
	metas := reg.Catalogue()
	out := make([]catalogueEntry, 0, len(metas))
	for _, meta := range metas {
		var requirement *scope.Requirement
		if advert, ok := meta.(handlers.AdvertDeclarer); ok {
			requirement = advert.AdvertisedRequirement()
		} else if declared, ok := meta.(handlers.RequirementDeclarer); ok {
			requirement = declared.Requirement()
		}
		out = append(out, catalogueEntry{
			fqn:         meta.Name(),
			role:        meta.Role(),
			effect:      handlers.EffectOf(meta),
			requirement: scope.AdvertisedRequirement(requirement, meta.Scope()),
		})
	}
	return out
}

// catalogueOfRegistry is the same projection of reg's executed handlers (those
// with a Func; nil reg, none), sorted by FQN: the catalogue NewPeerPublisher
// starts from. A registryShim answers Name, Role, Effect, Requirement and
// Scope from its handler, so an exported registry projects identically through
// either function.
func catalogueOfRegistry(reg *rpc.Registry) []catalogueEntry {
	if reg == nil {
		return nil
	}
	var out []catalogueEntry
	for _, h := range reg.All() {
		if h.Func == nil {
			continue
		}
		out = append(out, catalogueEntry{
			fqn:         h.FQN(),
			role:        h.Role(),
			effect:      h.Effect,
			requirement: scope.AdvertisedRequirement(h.Requirement, h.Scope),
		})
	}
	return out
}

// catalogueRoles is the role set a record carries for catalogue beside
// declared: their union, sorted, so every catalogued operation's role is
// advertised however the endpoint registered it.
func catalogueRoles(declared []string, catalogue []catalogueEntry) []string {
	seen := make(map[string]struct{}, len(declared)+len(catalogue))
	roles := make([]string, 0, len(declared)+len(catalogue))
	add := func(role string) {
		if role == "" {
			return
		}
		if _, dup := seen[role]; dup {
			return
		}
		seen[role] = struct{}{}
		roles = append(roles, role)
	}
	for _, role := range declared {
		add(role)
	}
	for _, entry := range catalogue {
		add(entry.role)
	}
	sort.Strings(roles)
	return roles
}
