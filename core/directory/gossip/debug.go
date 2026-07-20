/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import "github.com/bbmumford/loom/internal/debug"

var dbgGossip = debug.New("mesh.lad.gossip")
var dbgGossipDiscovery = debug.New("mesh.lad.gossip.discovery")
var dbgGossipPeer = debug.New("mesh.lad.gossip.peer")
var dbgGossipRatelimit = debug.New("mesh.lad.gossip.ratelimit")
var dbgGossipSync = debug.New("mesh.lad.gossip.sync")
var dbgGossipStream = debug.New("mesh.lad.gossip.aether")
