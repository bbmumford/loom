/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"os"
	"strings"
)

// defaultMeshHTTPPort is the HTTP/WS listen port assumed when the
// process doesn't set $PORT. Every fleet endpoint's fly.toml binds
// internal_port 8080; this is the fallback a peer applies when a reach
// record predates the http_port attribute. Consumed by
// peer_connections.go in the bestAddress fallback path.
const defaultMeshHTTPPort = "8080"

// meshHTTPPort returns the HTTP/WS port this process listens on. The
// endpoint binds its http.Server to $PORT; the mesh node lives in the
// same process, so reading the env var here is the source of truth
// without threading a config field through every endpoint's main.go.
func meshHTTPPort() string {
	if p := os.Getenv("PORT"); p != "" {
		return p
	}
	return defaultMeshHTTPPort
}

// SetRoles atomically updates the runtime's role set and triggers an
// immediate signed re-publish of the swarm PeerRecord. External callers
// (AI/Quorum shim, runtime promotion logic) use this to surface role
// changes to peers within the next gossip round.
func (rt *Runtime) SetRoles(roles []string) {
	rt.cfg.Roles = append([]string(nil), roles...)
	if rt.swarm != nil && rt.swarm.Publisher != nil {
		rt.swarm.Publisher.SetRoles(roles)
		rt.swarm.Publisher.PublishNow()
	}
}

// SetCapabilityMetadata publishes a free-form k=v capability bag to peers.
//
// The bag rides the signed PeerRecord's `Capabilities.extras`, so it reaches
// peers through the same authenticated channel as roles and addresses. Callers
// use it to advertise measured node properties — chip inventory, resident
// models, lane topology — that peers route on but that are not roles.
//
// 🔑 **Not roles.** A role is a promise to serve an RPC and `SelectPeer` acts
// on it; encoding capability values as roles would make every advertised
// number look like a servable endpoint. That distinction is why this is a
// separate channel rather than more entries in the role list.
func (rt *Runtime) SetCapabilityMetadata(metadata map[string]string) {
	if rt.swarm != nil && rt.swarm.Publisher != nil {
		rt.swarm.Publisher.SetCapabilityExtras(metadata)
	}
}

// SetServiceName atomically updates the runtime's service name and
// triggers an immediate signed re-publish. Rare — service name is
// usually fixed at boot — but useful for A/B blue-green rename.
func (rt *Runtime) SetServiceName(name string) {
	rt.cfg.ServiceName = name
	if rt.swarm != nil && rt.swarm.Publisher != nil {
		rt.swarm.Publisher.SetServiceName(name)
		rt.swarm.Publisher.PublishNow()
	}
}

// rolesJoined keeps the comma-joined form for legacy log lines that
// still print the role set. Used at runtime startup only.
func rolesJoined(roles []string) string {
	return strings.Join(roles, ",")
}
