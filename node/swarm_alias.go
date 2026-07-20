/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import swarmpb "github.com/bbmumford/swarm/proto/pb"

type swarmpbGrade = swarmpb.Grade

const (
	swarmBestEffort     = swarmpb.Grade_GRADE_BEST_EFFORT
	swarmReliable       = swarmpb.Grade_GRADE_RELIABLE
	swarmLowLatency     = swarmpb.Grade_GRADE_LOW_LATENCY
	swarmHighThroughput = swarmpb.Grade_GRADE_HIGH_THROUGHPUT
)
