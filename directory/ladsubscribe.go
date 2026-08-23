/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// The LAD adapter's subscription surface: the gap-free history->live handoff
// ports.LiveDirectory.Subscribe demands. Split out of ladlive.go to keep both
// files under the 500-line limit.
package directory

import (
	"context"
	"fmt"
	"sort"
	"sync"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/loom/ports"
)

// Subscribe streams accepted records with a gap-free history→live handoff.
//
// 🛑 IT DELIBERATELY DOES NOT USE ladcache.SubscribeWithReplay. That helper
// replays history and THEN registers the live subscriber, which is exactly
// the query-then-subscribe gap the port forbids: a record applied between the
// end of the replay and the registration is delivered to nobody, and a lost
// record in a directory feed is a node that never becomes reachable. Here the
// live subscriber is registered FIRST and its output is filtered to
// watermarks strictly past the history snapshot, so the overlap produces
// duplicates (which the filter drops) rather than a hole.
func (d *LADDirectory) Subscribe(ctx context.Context, topics []ports.Topic, from ports.Watermark) (<-chan ports.Record, error) {
	lts := ladProjectedTopics
	if len(topics) > 0 {
		seen := map[lad.Topic]bool{}
		lts = nil
		for _, t := range topics {
			for _, lt := range ladTopicsFor(t) {
				if !seen[lt] {
					seen[lt] = true
					lts = append(lts, lt)
				}
			}
		}
	}

	// The live tail carries each record's LAD watermark alongside it. It
	// cannot be recovered from the projected ports.Record: LAD's typed
	// records mostly carry no HLC, so ports.Record.HLC is 0 for them and the
	// real ordering scalar is the timestamp (see ladWatermark). Filtering the
	// tail on the projected HLC would compare 0 against the history floor and
	// silently discard every live record — a subscription that replays
	// history once and then goes quiet forever.
	type liveRecord struct {
		w ports.Watermark
		r ports.Record
	}
	live := make(chan liveRecord, 256)
	var liveMu sync.Mutex
	liveClosed := false
	sendLive := func(r lad.Record) {
		liveMu.Lock()
		defer liveMu.Unlock()
		if liveClosed {
			return
		}
		select {
		case live <- liveRecord{w: ladWatermark(r), r: r2p(r)}:
		default:
			// Buffer full: the port forbids a silent drop, and this
			// subscriber is resumable from its watermark, so stop feeding
			// rather than pretend. The consumer observes a closed channel
			// and resubscribes from its last watermark.
			liveClosed = true
			close(live)
		}
	}

	// 1. Live first — the overlap is duplicated, never lost.
	for _, lt := range lts {
		d.cache.Subscribe(lt, sendLive)
	}

	// 2. History snapshot, and the floor the live tail is filtered against.
	var history []ports.Record
	floor := from
	for _, lt := range lts {
		recs, err := d.cache.ChangesSinceHLC(lt, uint64(from))
		if err != nil {
			return nil, fmt.Errorf("directory: LAD subscribe %q: %w", lt, err)
		}
		for _, r := range recs {
			w := ladWatermark(r)
			if w <= from {
				continue
			}
			history = append(history, ladToPortRecord(r))
			if w > floor {
				floor = w
			}
		}
	}
	sort.Slice(history, func(i, j int) bool {
		if history[i].Topic != history[j].Topic {
			return history[i].Topic < history[j].Topic
		}
		return lessSlot(history[i], history[j])
	})

	out := make(chan ports.Record, 256)
	go func() {
		defer close(out)
		defer func() {
			liveMu.Lock()
			if !liveClosed {
				liveClosed = true
				close(live)
			}
			liveMu.Unlock()
		}()
		for _, r := range history {
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
		for {
			select {
			case lr, ok := <-live:
				if !ok {
					return
				}
				if lr.w <= floor {
					continue // already delivered as history
				}
				select {
				case out <- lr.r:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
