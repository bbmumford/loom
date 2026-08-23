/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "errors"

// Sentinel errors returned by the swarm integration helpers on Runtime.
var (
	ErrSwarmIdentityUnset  = errors.New("mesh/node: swarm requires Runtime identity to be loaded first")
	ErrSwarmNotInitialized = errors.New("mesh/node: swarm not initialized — call Runtime.InitSwarm first")
)
