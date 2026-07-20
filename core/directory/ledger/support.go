/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ledger

import (
	"log"
	"os"

	lad "github.com/bbmumford/ledger"
)

// subscriber represents a client subscribed to ledger updates.
type subscriber struct {
	id     int
	topics map[lad.Topic]struct{}
	ch     chan lad.Record
}

// dbgMesh is the debug logger for mesh ledger operations.
var dbgMesh = log.New(os.Stderr, "[mesh-ledger] ", log.LstdFlags)
