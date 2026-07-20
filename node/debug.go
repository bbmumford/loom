/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import "github.com/bbmumford/loom/internal/debug"

var dbgConfig = debug.New("mesh.node.config")
var dbgHTTP = debug.New("mesh.node.http")
var dbgNodeHealth = debug.New("mesh.node.health")
var dbgNodeIdentity = debug.New("mesh.node.identity")
var dbgNodeMetrics = debug.New("mesh.node.metrics")
var dbgNodeTLS = debug.New("mesh.node.tls")
var dbgRPC = debug.New("mesh.node.rpc")
var dbgResources = debug.New("mesh.node.resources")
var dbgSessionHealth = debug.New("mesh.node.session_health")
var dbgAether = debug.New("mesh.node.aether")
var dbgBidiRPC = debug.New("mesh.node.bidi_rpc")
var dbgScaler = debug.New("mesh.node.scaler")
var dbgForward = debug.New("mesh.node.forward")
var dbgPeers = debug.New("mesh.node.peers")
var dbgNode = debug.New("mesh.node.runtime")
var dbgTakeover = debug.New("mesh.node.takeover")
