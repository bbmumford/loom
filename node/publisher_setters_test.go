/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
)

// COVERAGE of what a node TELLS THE MESH ABOUT ITSELF: the
// PeerPublisher setters and the saturation-change republish, all at 0.0%.
//
// CENSUSED FIRST, per symbol, one level out, and checked for interface
// satisfaction:
//
//	SetRegion                          <- swarm_integration.go:110
//	SetMaxGrade                        <- swarm_integration.go:593
//	SetRoles                           <- reach_adapter.go + runtime.orbtr.ai/routes.go:399
//	SetServiceName (publisher)         <- reach_adapter.go:63's Runtime.SetServiceName
//	maybeRepublishOnSaturationChange   <- peer_publisher.go:325 (the run loop)
//
// 🔑 EVERY SETTER ENDS IN PublishNow(), which is a NON-BLOCKING send on a
// size-1 channel — so the observable contract is "the change is recorded AND a
// republish is pending", and rapid calls coalesce into one publish rather than
// queueing a burst onto the gossip topic.

// pendingRepublish reports whether a republish is queued, by draining the
// coalescing channel. Draining is the only way to observe it, and it is also
// how the run loop consumes the signal.
func pendingRepublish(p *PeerPublisher) bool {
	select {
	case <-p.republishCh:
		return true
	default:
		return false
	}
}

func publisherForTest() *PeerPublisher {
	// republishCh must be buffered exactly as NewPeerPublisher makes it, or the
	// coalescing assertions below would be about the fixture.
	return &PeerPublisher{republishCh: make(chan struct{}, 1)}
}

// 🔴 A SETTER THAT RECORDS WITHOUT REQUESTING A REPUBLISH IS SILENT: the node
// keeps advertising its old identity until the next 30s tick, and for
// SetRoles that means peers route to capabilities it no longer serves.
func TestEverySetterRecordsItsValueAndRequestsARepublish(t *testing.T) {
	for _, tc := range []struct {
		name   string
		set    func(*PeerPublisher)
		verify func(*testing.T, *PeerPublisher)
	}{
		{"service name", func(p *PeerPublisher) { p.SetServiceName("help-orbtr-io") },
			func(t *testing.T, p *PeerPublisher) {
				if p.serviceName != "help-orbtr-io" {
					t.Fatalf("serviceName = %q", p.serviceName)
				}
			}},
		{"region", func(p *PeerPublisher) { p.SetRegion("syd") },
			func(t *testing.T, p *PeerPublisher) {
				if p.region != "syd" {
					t.Fatalf("region = %q", p.region)
				}
			}},
		{"max grade", func(p *PeerPublisher) { p.SetMaxGrade(swarmpb.Grade_GRADE_LOW_LATENCY) },
			func(t *testing.T, p *PeerPublisher) {
				if p.maxGrade != swarmpb.Grade_GRADE_LOW_LATENCY {
					t.Fatalf("maxGrade = %v", p.maxGrade)
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := publisherForTest()
			tc.set(p)
			tc.verify(t, p)
			if !pendingRepublish(p) {
				t.Fatalf("%s was recorded but no republish was requested — the "+
					"node keeps advertising its previous value until the next "+
					"poll tick, and nothing tells it to re-emit", tc.name)
			}
		})
	}
}

// 🔑 PublishNow COALESCES. Ten rapid setter calls must leave ONE pending
// republish, not ten — the alternative is a burst onto the gossip topic every
// time a caller loops over a config map.
func TestRapidSetterCallsCoalesceIntoASingleRepublish(t *testing.T) {
	p := publisherForTest()
	for i := 0; i < 10; i++ {
		p.SetRegion("syd")
		p.SetServiceName("help-orbtr-io")
	}

	if !pendingRepublish(p) {
		t.Fatal("no republish pending after 20 setter calls")
	}
	if pendingRepublish(p) {
		t.Fatal("a SECOND republish was queued — 20 setter calls produced more " +
			"than one pending publish, so a caller looping over a config map " +
			"emits a burst onto the gossip topic")
	}
}

// SetRoles sorts, so two callers supplying the same set in different orders
// advertise byte-identical records — which is what stops a re-ordered slice
// from looking like a role change to every peer.
func TestSetRolesSortsSoEquivalentSetsAdvertiseIdentically(t *testing.T) {
	a, b := publisherForTest(), publisherForTest()
	a.SetRoles([]string{"anchor", "auth", "billing"})
	b.SetRoles([]string{"billing", "anchor", "auth"})

	if len(a.roles) != len(b.roles) {
		t.Fatalf("lengths differ: %v vs %v", a.roles, b.roles)
	}
	for i := range a.roles {
		if a.roles[i] != b.roles[i] {
			t.Fatalf("same set in different order produced %v and %v — the "+
				"published record differs byte-for-byte, so every peer sees a "+
				"role change that did not happen", a.roles, b.roles)
		}
	}
}

// SetRoles must copy: retaining the caller's slice would let a later mutation
// silently change what this node advertises.
func TestSetRolesDoesNotRetainTheCallersSlice(t *testing.T) {
	p := publisherForTest()
	in := []string{"anchor", "auth"}
	p.SetRoles(in)

	in[0] = "attacker-injected"
	for _, r := range p.Roles() {
		if r == "attacker-injected" {
			t.Fatal("mutating the caller's slice changed the advertised roles — " +
				"SetRoles retained the caller's array, so any later write by the " +
				"caller re-advertises this node with roles it never chose")
		}
	}
}

// ── The saturation bit ──────────────────────────────────────────────────────

// 🔑 REPUBLISH ONLY ON A FLIP. The doc is explicit: the bit must advertise and
// clear within the 30s poll window WITHOUT adding steady-state publish traffic.
// A republish on every poll would multiply gossip volume by the poll rate for
// every node in the mesh.
func TestSaturationRepublishesOnlyWhenTheBitFlips(t *testing.T) {
	p := publisherForTest()
	// A nil Runtime is the documented early return — Start()'s legacy path
	// leaves rt nil, and the run loop still calls this every poll.
	p.maybeRepublishOnSaturationChange()
	if pendingRepublish(p) {
		t.Fatal("a nil-Runtime publisher requested a republish — the poll loop " +
			"would emit on every tick with no saturation source at all")
	}

	// With a Runtime whose saturation is false and lastSatEmitted already
	// false, nothing changed, so nothing must publish.
	p.rt = &Runtime{}
	p.lastSatEmitted = false
	p.maybeRepublishOnSaturationChange()
	if pendingRepublish(p) {
		t.Fatal("an UNCHANGED saturation bit triggered a republish — every node " +
			"in the mesh would re-emit its record on every poll tick, which is " +
			"exactly the steady-state traffic the flip check exists to avoid")
	}

	// Now pretend we last emitted `true`: the current false is a flip.
	//
	// 🔑 THE FLIP PATH IS DIFFERENT FROM EVERY SETTER, and my first draft got
	// this wrong. Setters call PublishNow() (a coalesced signal); the flip calls
	// publishOnce() **synchronously and inline**, so the bit goes out inside the
	// poll window rather than waiting for the loop. That means the flip does NOT
	// coalesce with a concurrent setter republish — deliberate, and the reason
	// this test needs a publisher that can actually publish.
	p.node = &capturingNode{}
	p.lastSatEmitted = true
	p.maybeRepublishOnSaturationChange()
	if p.lastSatEmitted {
		t.Fatal("the flip was not recorded — lastSatEmitted still reads true, so " +
			"the next poll sees the same 'change' and republishes forever")
	}
}

// 🔴 THE SECOND HALF OF MESH-G07, WHICH THE ORIGINAL FIX MISSED.
//
// publishOnce's comment says the p.rt guard exists because "a publisher
// constructed with rt==nil panicked on its first publish … this only affects
// that fallback / test paths". The guard checked p.rt and stopped there, so a
// HALF-BUILT Runtime — non-nil, identity not yet assigned — still panicked on
// exactly that path. Measured: my first version of the test above crashed at
// peer_publisher.go:484 with a bare &Runtime{}.
//
// Production assigns identity during Initialize before the publisher is
// constructed, so completing the guard changes no live behaviour. This is the
// regression pin.
func TestPublishingWithAHalfBuiltRuntimeDoesNotPanic(t *testing.T) {
	p := publisherForTest()
	p.node = &capturingNode{}
	p.rt = &Runtime{} // non-nil, identity == nil

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("publishOnce panicked with a half-built Runtime: %v — the "+
				"MESH-G07 guard checks p.rt but not p.rt.identity, which is one "+
				"dereference short of what its own comment promises", r)
		}
	}()
	p.publishOnce()
}

// And with no Runtime at all — the documented bare-goroutine fallback — the
// record must still publish, carrying an empty node id rather than crashing.
func TestPublishingWithNoRuntimeAtAllStillEmitsARecord(t *testing.T) {
	p := publisherForTest()
	n := &capturingNode{}
	p.node = n

	p.publishOnce()

	if len(n.body) == 0 {
		t.Fatal("no record was published by a Runtime-less publisher — the " +
			"documented fallback path emits nothing at all")
	}
}

// The recorded value must track the observed one even when it does not flip, or
// the first genuine flip is missed.
func TestTheLastEmittedBitIsRecordedOnEveryObservation(t *testing.T) {
	p := publisherForTest()
	p.rt = &Runtime{}
	p.lastSatEmitted = true

	p.maybeRepublishOnSaturationChange() // observes false, flips
	if p.lastSatEmitted {
		t.Fatal("lastSatEmitted was not updated on a flip")
	}
	_ = pendingRepublish(p) // drain

	p.maybeRepublishOnSaturationChange() // observes false again, no flip
	if pendingRepublish(p) {
		t.Fatal("the second identical observation republished — the recorded bit " +
			"is not being consulted")
	}
}
