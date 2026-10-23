/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/core/directory/gossip"
	"github.com/bbmumford/loom/core/streams"
)

// meshWireVersions returns the application entries of the wire version offer
// every mesh session of this runtime sends in its aether handshake: the
// gossip codec and reconciliation profile (gossip.WireVersions) and the
// stream registry (streams.WireVersions), each required, beside app, the
// caller's own entries. An entry of app that uses one of loom's names is
// replaced by loom's, because AcceptMeshConnection runs gossip under the
// profile those names select.
//
// Initialize puts the result on the base NoiseTransportConfig, so every VL1
// Noise transport offers it in its handshake, and SetupMeshSession puts it on
// the byte-stream sessions' options, which a caller's SetSessionOptions does
// not carry. A peer whose build speaks another version of any of the three is
// refused before a stream exists.
func meshWireVersions(app aether.WireVersions) aether.WireVersions {
	own := append(gossip.WireVersions(), streams.WireVersions()...)
	out := make(aether.WireVersions, 0, len(app)+len(own))
	for _, e := range app {
		loom := false
		for _, o := range own {
			if e.Name == o.Name {
				loom = true
				break
			}
		}
		if !loom {
			out = append(out, e)
		}
	}
	return append(out, own...)
}

// meshSessionWireVersions returns the set a mesh session negotiated, or the
// empty set for a session that reports none, which selects no gossip profile.
// On a byte-stream session aether.SessionWireVersions returns only when the
// preface exchange has ended, so awaitMeshSessionProfile, which SetupMeshSession
// runs before it counts a session, reads the exchange's result and not an
// empty set the exchange has yet to fill.
func meshSessionWireVersions(s aether.Session) aether.NegotiatedWireVersions {
	set, _ := aether.SessionWireVersions(s)
	return set
}
