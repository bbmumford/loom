/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package gateway

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	task "github.com/bbmumford/loom/core/task"
	bidding "github.com/bbmumford/loom/core/task/bidding"
)

// awardClock is a clock the test moves; the offer pump still ticks in real
// time and reads it, so a window closes when the test says it has.
type awardClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *awardClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *awardClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// cheapestRule awards the lowest "price" a bid's metadata names; a bid naming
// none is ineligible. ghost, when set, is the node it names instead.
type cheapestRule struct{ ghost string }

func (r cheapestRule) Award(offer task.TaskOffer, bids []task.Bid, _ bidding.AwardContext) (bidding.AwardDecision, error) {
	d := bidding.AwardDecision{Rule: "test.cheapest", Candidates: bids}
	if r.ghost != "" {
		d.Winner = &task.Bid{TaskID: offer.Task.ID, NodeID: r.ghost}
		return d, nil
	}
	best := -1
	for i, b := range bids {
		p, err := strconv.Atoi(b.Metadata["price"])
		if err != nil {
			continue
		}
		if best < 0 || p < mustPrice(bids[best]) {
			best = i
		}
	}
	if best < 0 {
		d.Reason = "no bid names a price"
		return d, nil
	}
	w := bids[best]
	d.Winner = &w
	return d, nil
}

func mustPrice(b task.Bid) int {
	p, _ := strconv.Atoi(b.Metadata["price"])
	return p
}

// awardFixture is a started gateway over clock with rule, a one-second bid
// window and a five-second offer, recording every decision.
type awardFixture struct {
	gw        *Gateway
	clock     *awardClock
	offer     task.TaskOffer
	mu        sync.Mutex
	decisions []bidding.AwardDecision
}

func newAwardFixture(t *testing.T, rule bidding.AwardRule) *awardFixture {
	t.Helper()
	f := &awardFixture{clock: &awardClock{now: time.Unix(10_000, 0)}}
	gw, err := NewGateway(GatewayConfig{ID: "gw-award", BidWindow: time.Second, OfferTTL: 5 * time.Second},
		WithClock(f.clock), WithAwardRule(rule),
		WithAwardRecorder(func(_ task.TaskOffer, d bidding.AwardDecision) {
			f.mu.Lock()
			f.decisions = append(f.decisions, d)
			f.mu.Unlock()
		}))
	if err != nil {
		t.Fatal(err)
	}
	f.gw = gw
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); gw.Stop() })
	gw.Start(ctx)
	if err := gw.Enqueue(&task.Task{ID: "job-1", Partition: "p", Role: "role"}); err != nil {
		t.Fatal(err)
	}
	select {
	case f.offer = <-gw.Offers():
	case <-time.After(2 * time.Second):
		t.Fatal("no offer")
	}
	return f
}

func (f *awardFixture) bid(node, price string) {
	b := task.Bid{TaskID: f.offer.Task.ID, OfferToken: f.offer.OfferToken, NodeID: node}
	if price != "" {
		b.Metadata = map[string]string{"price": price}
	}
	f.gw.BidSink() <- b
}

func (f *awardFixture) lastDecision(t *testing.T) bidding.AwardDecision {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		f.mu.Lock()
		n := len(f.decisions)
		var d bidding.AwardDecision
		if n > 0 {
			d = f.decisions[n-1]
		}
		f.mu.Unlock()
		if n > 0 {
			return d
		}
	}
	t.Fatal("no award decision was recorded")
	return bidding.AwardDecision{}
}

func (f *awardFixture) noAssignmentFor(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case a := <-f.gw.Assignments():
		t.Fatalf("assignment to %s issued, want none yet", a.Primary)
	case <-time.After(d):
	}
}

func (f *awardFixture) assignment(t *testing.T) task.Assignment {
	t.Helper()
	select {
	case a := <-f.gw.Assignments():
		return a
	case <-time.After(2 * time.Second):
		t.Fatal("no assignment")
		return task.Assignment{}
	}
}

// Bids arriving inside the window are all collected, none wins before it closes,
// and when it closes the rule decides once over every one of them: the cheapest
// wins and the recorder holds the rule, the winner and all three candidates.
func TestTheBidWindowAwardsTheRulesWinnerOverEveryBidItCollected(t *testing.T) {
	f := newAwardFixture(t, cheapestRule{})
	f.bid("node-a", "5")
	f.bid("node-b", "3")
	f.bid("node-c", "4")
	f.noAssignmentFor(t, 200*time.Millisecond)

	f.clock.advance(time.Second)
	a := f.assignment(t)
	if a.Primary != "node-b" {
		t.Fatalf("assignment to %s, want node-b, the cheapest", a.Primary)
	}
	d := f.lastDecision(t)
	if d.Rule != "test.cheapest" || d.Winner == nil || d.Winner.NodeID != "node-b" || len(d.Candidates) != 3 {
		t.Fatalf("recorded decision = %+v, want the rule, node-b and three candidates", d)
	}
}

// A node's later bid on the same offer replaces its earlier one, so the window
// holds one bid per node and the revised price is the one decided on.
func TestALaterBidFromTheSameNodeReplacesItsEarlierOne(t *testing.T) {
	f := newAwardFixture(t, cheapestRule{})
	f.bid("node-a", "5")
	f.bid("node-b", "3")
	f.bid("node-a", "2")
	f.noAssignmentFor(t, 100*time.Millisecond)
	f.clock.advance(time.Second)
	if a := f.assignment(t); a.Primary != "node-a" {
		t.Fatalf("assignment to %s, want node-a at its revised price", a.Primary)
	}
	if d := f.lastDecision(t); len(d.Candidates) != 2 {
		t.Fatalf("candidates = %d, want one per node", len(d.Candidates))
	}
}

// A bid naming another offer of the task is dropped, while a bid with no token
// answers the open offer.
func TestABidNamingAnotherOfferIsDropped(t *testing.T) {
	f := newAwardFixture(t, cheapestRule{})
	f.gw.BidSink() <- task.Bid{TaskID: f.offer.Task.ID, OfferToken: "lapsed-offer", NodeID: "node-stale",
		Metadata: map[string]string{"price": "1"}}
	f.gw.BidSink() <- task.Bid{TaskID: f.offer.Task.ID, NodeID: "node-untokened",
		Metadata: map[string]string{"price": "7"}}
	f.noAssignmentFor(t, 100*time.Millisecond)
	f.clock.advance(time.Second)
	if a := f.assignment(t); a.Primary != "node-untokened" {
		t.Fatalf("assignment to %s, want node-untokened: the stale bid must not be collected", a.Primary)
	}
}

// With no eligible bid when the window closes, nothing is assigned and the
// offer stays open; a later eligible bid is then decided at once.
func TestNoEligibleBidLeavesTheOfferOpenForALaterBid(t *testing.T) {
	f := newAwardFixture(t, cheapestRule{})
	f.bid("node-a", "")
	f.clock.advance(time.Second)
	if d := f.lastDecision(t); d.Winner != nil || d.Reason == "" {
		t.Fatalf("decision = %+v, want no winner with a reason", d)
	}
	f.noAssignmentFor(t, 150*time.Millisecond)
	f.bid("node-b", "6")
	if a := f.assignment(t); a.Primary != "node-b" {
		t.Fatalf("assignment to %s, want node-b", a.Primary)
	}
}

// A rule naming a node that bid on nothing awards nothing, and the recorded
// decision says so, so a rule cannot assign a node no bid in the window carried.
func TestARuleNamingANodeThatDidNotBidAwardsNothing(t *testing.T) {
	f := newAwardFixture(t, cheapestRule{ghost: "node-ghost"})
	f.bid("node-a", "2")
	f.clock.advance(time.Second)
	d := f.lastDecision(t)
	if d.Winner != nil || d.Reason == "" {
		t.Fatalf("decision = %+v, want no winner with a reason", d)
	}
	f.noAssignmentFor(t, 150*time.Millisecond)
}
