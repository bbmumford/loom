/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
)

// — SCOPING THE RESPONSE-DEDUP CACHE KEY (the coding contract).
//
// The cache was keyed on the BARE req().Id: a caller-supplied wire string, in one
// process-global namespace, read at handleRequest step 0a before any principal
// is bound and written for every successful local execution with a 10s TTL.
// Anything sharing an id collided regardless of who sent it or what it called.
//
// 🔴 THE IDS ARE NOT EVEN ADVERSARIAL IN PRACTICE. pkg/dispatch mints request
// ids from a process-global base36 counter (hwp_dispatch.go:231), so two nodes
// that boot together independently produce the same low ids. Cross-node
// response substitution needed no attacker, only a coincidence that the design
// made likely.
//
// The four properties below are the whole contract. The THIRD is the one that
// makes this a fix rather than a removal: scoping a key is only correct if
// legitimate dedup still works afterwards.

// dedupProbe counts executions and reports which handler ran.
type dedupProbe struct {
	name  string
	calls int32
}

func (h *dedupProbe) Name() string                      { return h.name }
func (h *dedupProbe) Role() string                      { return "orbtr.test" }
func (h *dedupProbe) RequiresAuth() bool                { return false }
func (h *dedupProbe) AllowedAuthTypes() []string        { return nil }
func (h *dedupProbe) Scopes() []string                  { return nil }
func (h *dedupProbe) TenantScope() handlers.TenantScope { return handlers.TenantScopeNone }
func (h *dedupProbe) AllowedTenants() []string          { return nil }
func (h *dedupProbe) ExecuteRPC(context.Context, *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	atomic.AddInt32(&h.calls, 1)
	return &handlers.RPCResponse{Success: true, Payload: []byte(h.name)}, nil
}

func dedupServer(t *testing.T, names ...string) (*RPCServer, map[string]*dedupProbe) {
	t.Helper()
	registry := handlers.NewHandlerRegistry()
	probes := map[string]*dedupProbe{}
	for _, n := range names {
		p := &dedupProbe{name: n}
		if err := registry.RegisterRPC(p); err != nil {
			t.Fatalf("register %s: %v", n, err)
		}
		probes[n] = p
	}
	return NewRPCServer(registry), probes
}

func fromPeer(peer string) context.Context {
	return withCallerNode(context.Background(), peer)
}

// 🔴🔴 THE HEADLINE DEFECT: ONE PEER'S RESPONSE SERVED TO ANOTHER.
// Both peers send the same request id for the same handler. Before the fix the
// second peer received the first peer's cached response and the handler never
// ran for it — a cross-peer response substitution decided purely by string
// equality on a wire-supplied field.
func TestTwoPeersSharingARequestIDDoNotShareAResponse(t *testing.T) {
	s, probes := dedupServer(t, "orbtr.test.Dedup")
	req := func() *pb.RPCRequest {
		return &pb.RPCRequest{Id: "42", Handler: "orbtr.test.Dedup"}
	}

	if r := s.handleRequest(fromPeer("peer-a"), req()); !r.Success {
		t.Fatalf("peer-a call failed: %s", r.Error)
	}
	if r := s.handleRequest(fromPeer("peer-b"), req()); !r.Success {
		t.Fatalf("peer-b call failed: %s", r.Error)
	}

	if got := atomic.LoadInt32(&probes["orbtr.test.Dedup"].calls); got != 2 {
		t.Errorf("handler ran %d times for two DIFFERENT peers using the same request "+
			"id, want 2 — peer-b was served peer-a's cached response, so a response "+
			"computed for one peer's request crossed to another's", got)
	}
}

// 🔴 CROSS-HANDLER SUBSTITUTION. Same peer, same id, two different handlers.
// Before the fix the second handler never ran and the caller received the
// first handler's payload — an auth response satisfying a DHCP call.
func TestOneIDAcrossTwoHandlersDoesNotCrossResponses(t *testing.T) {
	s, probes := dedupServer(t, "orbtr.test.Alpha", "orbtr.test.Beta")
	ctx := fromPeer("peer-a")

	first := s.handleRequest(ctx, &pb.RPCRequest{Id: "42", Handler: "orbtr.test.Alpha"})
	second := s.handleRequest(ctx, &pb.RPCRequest{Id: "42", Handler: "orbtr.test.Beta"})

	if !first.Success || !second.Success {
		t.Fatalf("calls failed: %q / %q", first.Error, second.Error)
	}
	if got := atomic.LoadInt32(&probes["orbtr.test.Beta"].calls); got != 1 {
		t.Errorf("Beta ran %d times, want 1 — it was served Alpha's cached response, so "+
			"one handler's output answered a different handler's request", got)
	}
	if string(second.Payload) == "orbtr.test.Alpha" {
		t.Error("the response to a Beta request carries Alpha's payload — cross-handler " +
			"response substitution")
	}
}

// 🔴 NO DESCOPING. The whole point of the cache is that parallel probes of ONE
// logical request execute the handler once. Narrowing the key is only a fix if
// this still holds — otherwise the "fix" is a removal, and every duplicate
// probe re-executes a handler that already ran.
func TestTheSamePeerRepeatingOneRequestStillDedups(t *testing.T) {
	s, probes := dedupServer(t, "orbtr.test.Dedup")
	ctx := fromPeer("peer-a")
	req := func() *pb.RPCRequest {
		return &pb.RPCRequest{Id: "42", Handler: "orbtr.test.Dedup"}
	}

	for i := 0; i < 4; i++ {
		if r := s.handleRequest(ctx, req()); !r.Success {
			t.Fatalf("call %d failed: %s", i, r.Error)
		}
	}

	if got := atomic.LoadInt32(&probes["orbtr.test.Dedup"].calls); got != 1 {
		t.Errorf("handler ran %d times for four identical probes from ONE peer, want 1 — "+
			"dedup no longer works, so scoping the key removed the capability instead of "+
			"correcting it", got)
	}
}

// 🔬 THIS TEST EXISTS BECAUSE A MUTANT SURVIVED. Dropping req().Id from
// the key entirely — dedupCacheKey(caller, handler, "") — passed every other
// test in this file. Each of them varies the CALLER or the HANDLER, so the two
// scoping dimensions I added were covered while the one dimension that was
// already there was not.
//
// That is the failure mode of a scoping fix: attention goes to the dimensions
// being added and the original key component becomes untested. Without req().Id
// a peer's SECOND, genuinely different request is answered with its FIRST
// response — strictly worse than the bug being fixed, because it needs no
// second peer and no collision at all.
//
// TestEachKeyDimensionSeparatesIndependently covers this at the key function,
// but the mutant lives at the CALL SITE, where the key function's own tests
// cannot see it. Both levels are needed.
func TestTwoDifferentRequestsFromOnePeerAreNotConflated(t *testing.T) {
	s, probes := dedupServer(t, "orbtr.test.Dedup")
	ctx := fromPeer("peer-a")

	first := s.handleRequest(ctx, &pb.RPCRequest{Id: "req()-1", Handler: "orbtr.test.Dedup"})
	second := s.handleRequest(ctx, &pb.RPCRequest{Id: "req()-2", Handler: "orbtr.test.Dedup"})

	if !first.Success || !second.Success {
		t.Fatalf("calls failed: %q / %q", first.Error, second.Error)
	}
	if got := atomic.LoadInt32(&probes["orbtr.test.Dedup"].calls); got != 2 {
		t.Errorf("handler ran %d times for two DIFFERENT request ids from one peer, want "+
			"2 — the request id is absent from the cache key, so every request a peer "+
			"makes to a handler is answered with the first response it ever got", got)
	}
}

// Requests with no peer session (no caller in ctx) must still dedup among
// themselves — the key degrades to (handler, id) rather than failing.
func TestRequestsWithNoCallerStillDedupAmongThemselves(t *testing.T) {
	s, probes := dedupServer(t, "orbtr.test.Dedup")
	req := func() *pb.RPCRequest {
		return &pb.RPCRequest{Id: "42", Handler: "orbtr.test.Dedup"}
	}

	s.handleRequest(context.Background(), req())
	s.handleRequest(context.Background(), req())

	if got := atomic.LoadInt32(&probes["orbtr.test.Dedup"].calls); got != 1 {
		t.Errorf("handler ran %d times for two identical callerless probes, want 1", got)
	}
}

// ─── the key function itself ─────────────────────────────────────────────

// 🔴 INJECTIVITY IS THE SECURITY PROPERTY, NOT A TIDINESS ONE. requestID is
// fully caller-controlled. With any fixed delimiter a caller could choose an id
// that reproduces another (caller, handler) pair's key and deliberately read a
// response it was never entitled to — turning an accidental collision into a
// targeted one. Length prefixes make the encoding injective.
//
// 🔬 EVERY PAIR BELOW IS CHOSEN SO A NAIVE "a|b|c" JOIN WOULD COLLIDE. Against
// a delimiter-joined key these inputs produce identical strings; the test only
// has force because the fixtures are built to defeat the weaker scheme.
func TestTheDedupKeyCannotBeForgedByAChosenRequestID(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    [3]string
		b    [3]string
	}{
		{
			name: "id absorbs the handler",
			a:    [3]string{"peer-a", "orbtr.test.Alpha", "42"},
			b:    [3]string{"peer-a", "orbtr", ".test.Alpha|42"},
		},
		{
			name: "id absorbs the caller",
			a:    [3]string{"peer-a", "h", "x"},
			b:    [3]string{"peer", "-a", "h|x"},
		},
		{
			name: "empty component vs adjacent boundary",
			a:    [3]string{"", "ab", "c"},
			b:    [3]string{"", "a", "bc"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ka := dedupCacheKey(tc.a[0], tc.a[1], tc.a[2])
			kb := dedupCacheKey(tc.b[0], tc.b[1], tc.b[2])
			if ka == kb {
				t.Errorf("two different (caller, handler, id) triples produced the same "+
					"key %q — a caller can choose a request id that lands on another "+
					"peer-or-handler's cache entry, which converts an accidental "+
					"collision into a targeted read", ka)
			}
		})
	}
}

// Each dimension must independently separate keys; a key that ignored one of
// the three would still pass a test that only varied the others.
func TestEachKeyDimensionSeparatesIndependently(t *testing.T) {
	base := dedupCacheKey("peer-a", "handler-1", "id-1")
	for _, tc := range []struct {
		dim string
		key string
	}{
		{"caller", dedupCacheKey("peer-b", "handler-1", "id-1")},
		{"handler", dedupCacheKey("peer-a", "handler-2", "id-1")},
		{"requestID", dedupCacheKey("peer-a", "handler-1", "id-2")},
	} {
		if tc.key == base {
			t.Errorf("changing %s alone did not change the key — that dimension is "+
				"absent from the key, so responses cross it freely", tc.dim)
		}
	}
	if again := dedupCacheKey("peer-a", "handler-1", "id-1"); again != base {
		t.Errorf("the key is not deterministic: %q then %q", base, again)
	}
}

// A large sweep for accidental collisions on realistic inputs.
func TestNoCollisionsAcrossAThousandTriples(t *testing.T) {
	seen := map[string][3]string{}
	for c := 0; c < 10; c++ {
		for h := 0; h < 10; h++ {
			for i := 0; i < 10; i++ {
				trip := [3]string{
					fmt.Sprintf("peer-%d", c),
					fmt.Sprintf("orbtr.test.H%d", h),
					fmt.Sprintf("%d", i),
				}
				k := dedupCacheKey(trip[0], trip[1], trip[2])
				if prev, dup := seen[k]; dup {
					t.Fatalf("collision: %v and %v both key to %q", prev, trip, k)
				}
				seen[k] = trip
			}
		}
	}
}

// Replaces a weaker assertion that a mutant survived.
//
// A mutant that made every length prefix a literal "0" — reducing the
// scheme to a plain join on the delimiter "0:" — passed
// TestTheDedupKeyCannotBeForgedByAChosenRequestID, because those fixtures were
// hand-built to collide under a "|" join specifically. They tested ONE weaker
// scheme I had imagined, not the property.
//
// The property is injectivity: distinct triples must never share a key. This
// sweeps an alphabet deliberately seeded with the characters the encoding
// itself uses (digits and ':') plus the empty string, so any fixed-delimiter
// join — whatever delimiter it picks — produces a collision here. It is the
// general form of what the hand-picked cases were reaching for.
func TestTheKeyEncodingIsInjectiveOverAHostileAlphabet(t *testing.T) {
	// Contains the encoding's own separator and length characters, so a
	// component can imitate a boundary if the scheme lets it.
	alphabet := []string{"", "a", ":", "0", "0:", "a0:", ":0", "10:", "1:a"}

	seen := map[string][3]string{}
	for _, c := range alphabet {
		for _, h := range alphabet {
			for _, i := range alphabet {
				trip := [3]string{c, h, i}
				k := dedupCacheKey(c, h, i)
				if prev, dup := seen[k]; dup {
					t.Fatalf("NOT INJECTIVE: %q and %q both key to %q — a caller who "+
						"controls the request id can construct one that lands on another "+
						"peer's or handler's cache entry, turning an accidental collision "+
						"into a targeted read of a response it was never entitled to",
						prev, trip, k)
				}
				seen[k] = trip
			}
		}
	}
	if want := len(alphabet) * len(alphabet) * len(alphabet); len(seen) != want {
		t.Errorf("produced %d distinct keys from %d distinct triples", len(seen), want)
	}
}
