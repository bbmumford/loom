/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package node

import "errors"

// Sentinel errors returned by the swarm integration helpers on Runtime.
var (
	ErrSwarmIdentityUnset  = errors.New("node: swarm requires Runtime identity to be loaded first")
	ErrSwarmNotInitialized = errors.New("node: swarm not initialized — call Runtime.InitSwarm first")
)
