/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sort"
	"strings"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/pkg/rpc/effect"
)

// The peer record tags that advertise the effect class of the operations a
// node executes, each a comma-joined list of FQNs. PeerPublisher writes them
// from the rpc.Registry it publishes (effectTags); RoleTable reads them back
// through PeerInfo.Tags (RoleTable.HandlerEffect). An FQN in neither list is
// effect.Unsafe, so a record carrying neither tag holds every operation it
// lists to the one-execution rule. An FQN never holds a comma, which the
// registry's namespace.domain.Operation shape rules out.
const (
	readOnlyHandlersMetaKey   = "readonly_handlers"
	idempotentHandlersMetaKey = "idempotent_handlers"
)

// effectTags returns the two effect tags for the executed handlers in
// registered: those with a Func, the same set publishOnce lists in
// RpcHandlers. A tag with no FQN is left out.
func effectTags(registered []*rpc.Handler) []string {
	var readOnly, idempotent []string
	for _, h := range registered {
		if h.Func == nil {
			continue
		}
		switch h.Effect {
		case effect.ReadOnly:
			readOnly = append(readOnly, h.FQN())
		case effect.Idempotent:
			idempotent = append(idempotent, h.FQN())
		}
	}
	var tags []string
	if len(readOnly) > 0 {
		sort.Strings(readOnly)
		tags = append(tags, readOnlyHandlersMetaKey+"="+strings.Join(readOnly, ","))
	}
	if len(idempotent) > 0 {
		sort.Strings(idempotent)
		tags = append(tags, idempotentHandlersMetaKey+"="+strings.Join(idempotent, ","))
	}
	return tags
}

// advertisedEffect returns the class info's effect tags give handler.
func advertisedEffect(info PeerInfo, handler string) effect.Class {
	if listsFQN(info.Tags[readOnlyHandlersMetaKey], handler) {
		return effect.ReadOnly
	}
	if listsFQN(info.Tags[idempotentHandlersMetaKey], handler) {
		return effect.Idempotent
	}
	return effect.Unsafe
}

func listsFQN(list, fqn string) bool {
	for _, f := range strings.Split(list, ",") {
		if f == fqn {
			return true
		}
	}
	return false
}

// HandlerEffect returns the least repeatable class (effect.Least) that the
// peers advertising handler under role give it, and false when no peer
// advertises it. Taking the least means one holder that declares the
// operation Unsafe, or declares nothing for it, keeps every caller to one
// execution.
func (t *RoleTable) HandlerEffect(role, handler string) (effect.Class, bool) {
	nodes := t.Lookup(role, handler)
	if len(nodes) == 0 {
		return effect.Unsafe, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	class := effect.ReadOnly
	for _, n := range nodes {
		class = effect.Least(class, advertisedEffect(t.infoBy[n.NodeID], handler))
	}
	return class, true
}

// HandlerEffect implements dispatch.EffectFinder for HWPCaller: the class the
// RoleTable's holders of handler advertise, and effect.Unsafe when the swarm
// is not running or no peer advertises it.
func (m *ConnectionManager) HandlerEffect(role, handler string) effect.Class {
	if m == nil || m.rt == nil || m.rt.swarm == nil || m.rt.swarm.RoleTable == nil {
		return effect.Unsafe
	}
	class, _ := m.rt.swarm.RoleTable.HandlerEffect(role, handler)
	return class
}

// handlerEffect is the class the forwarder holds handler to: the least of the
// class this node's own registry declares for it, when this node registers
// it, and the class its advertising peers give it. A handler neither knows is
// effect.Unsafe.
func (rt *Runtime) handlerEffect(handler string) effect.Class {
	class, known := effect.ReadOnly, false
	if reg := rt.Registry(); reg != nil {
		if meta, ok := reg.GetMeta(handler); ok {
			class, known = effect.Least(class, handlers.EffectOf(meta)), true
		}
	}
	if rt.swarm != nil && rt.swarm.RoleTable != nil {
		if c, ok := rt.swarm.RoleTable.HandlerEffect(roleOfFQN(handler), handler); ok {
			class, known = effect.Least(class, c), true
		}
	}
	if !known {
		return effect.Unsafe
	}
	return class
}

// roleOfFQN is the role an FQN is advertised under: everything before its
// last dot, the split Forward makes.
func roleOfFQN(fqn string) string {
	if dot := strings.LastIndexByte(fqn, '.'); dot > 0 {
		return fqn[:dot]
	}
	return fqn
}
