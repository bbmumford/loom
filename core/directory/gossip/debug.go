/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package gossip

import "github.com/bbmumford/loom/internal/debug"

var dbgGossip = debug.New("mesh.lad.gossip")
var dbgGossipDiscovery = debug.New("mesh.lad.gossip.discovery")
var dbgGossipPeer = debug.New("mesh.lad.gossip.peer")
var dbgGossipRatelimit = debug.New("mesh.lad.gossip.ratelimit")
var dbgGossipSync = debug.New("mesh.lad.gossip.sync")
var dbgGossipStream = debug.New("mesh.lad.gossip.aether")
