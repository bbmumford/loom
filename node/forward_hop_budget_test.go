/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// A caller whose deadline is the default a call over the hop's session gets
// (adaptiveRPCTimeout) reaches the forwarder with that deadline less what the
// first hop spent. The forward is sent: the hop's need is its share of that
// default for the hops the request may still take, not the whole default,
// which a caller that gave the default can never have left one hop on.
func TestAForwardOfACallGivenTheDefaultDeadlineIsSentOneHopOn(t *testing.T) {
	for _, leg := range []string{"re-route", "route list"} {
		t.Run(leg, func(t *testing.T) {
			hop := &hopSession{remote: "target", proto: aether.ProtoNoise}
			f := forwarderWith(hop)
			ctx, cancel := context.WithTimeout(context.Background(), adaptiveRPCTimeout(hop)-50*time.Millisecond)
			defer cancel()
			req := forwardRequest("target", "")
			if leg == "route list" {
				req.RouteList = []string{"target"}
			}
			if _, err := f.Forward(ctx, req); err != nil {
				t.Fatalf("Forward refused a call given the default deadline one hop on: %v", err)
			}
			if n := hop.sends(); n != 1 {
				t.Fatalf("the next hop received %d request(s), want 1", n)
			}
		})
	}
}

// A hop's need is the session's default call deadline divided across the most
// hops a request can take (the origin's send and pb.MaxRPCHops forwards), times
// the hops this forward and every later one may still take: it shrinks as the
// request travels, so the time a caller gave covers each hop in turn. Just
// below the need at each hop count the forward is refused typed, naming that
// need, and nothing is sent; just above it the forward is sent.
func TestAHopsNeedIsItsShareOfTheDefaultDeadlineForTheHopsLeft(t *testing.T) {
	for hops := int32(1); hops <= pb.MaxRPCHops; hops++ {
		probe := &hopSession{remote: "target", proto: aether.ProtoNoise}
		want := adaptiveRPCTimeout(probe) * time.Duration(pb.MaxRPCHops-int(hops)+1) / time.Duration(pb.MaxRPCHops+1)

		short := &hopSession{remote: "target", proto: aether.ProtoNoise}
		ctx, cancel := context.WithTimeout(context.Background(), want-200*time.Millisecond)
		req := forwardRequest("target", "")
		req.Hops = hops
		_, err := forwarderWith(short).Forward(ctx, req)
		cancel()
		var rde *RouteDeadlineError
		if !errors.As(err, &rde) || rde.Need != want {
			t.Fatalf("hop %d with %v left: Forward returned %v; want a refusal naming the need %v",
				hops, want-200*time.Millisecond, err, want)
		}
		if n := short.sends(); n != 0 {
			t.Fatalf("hop %d: the next hop received %d request(s) the budget does not cover", hops, n)
		}

		enough := &hopSession{remote: "target", proto: aether.ProtoNoise}
		ctx, cancel = context.WithTimeout(context.Background(), want+200*time.Millisecond)
		req = forwardRequest("target", "")
		req.Hops = hops
		_, err = forwarderWith(enough).Forward(ctx, req)
		cancel()
		if err != nil {
			t.Fatalf("hop %d with %v left: Forward refused with %v; the need is %v", hops, want+200*time.Millisecond, err, want)
		}
		if n := enough.sends(); n != 1 {
			t.Fatalf("hop %d: the next hop received %d request(s), want 1", hops, n)
		}
	}
}

// Over real sessions, a node holding no session to the node serving an
// operation calls it with the deadline the endpoint test helpers give (5 s), and
// the seed both hold sessions to forwards it: the call is answered by the
// serving node, not refused for the deadline.
func TestACallGivenFiveSecondsTakesTheRelayedPath(t *testing.T) {
	if testing.Short() {
		t.Skip("starts three mesh runtimes")
	}
	seed := relayTestRuntime(t, "seed", nil)
	host, roots := seedJoinTestServer(t, seed)
	publishRelayTestOp(t, seed, "AtSeed")
	early := relayTestRuntime(t, "early", nil)
	publishRelayTestOp(t, early, "AtEarly")
	joinRelayTestSeed(t, early, host, roots)
	late := relayTestRuntime(t, "late", nil)
	publishRelayTestOp(t, late, "AtLate")
	joinRelayTestSeed(t, late, host, roots)
	waitRelayTest(t, 60*time.Second, "the later joiner to hold the earlier joiner's advert", func() bool {
		return holdsRelayTestAdvert(late, "AtEarly", early)
	})

	caller := dispatch.NewHWPCaller(late.ConnManager())
	t.Cleanup(caller.Close)
	payload, err := proto.Marshal(wrapperspb.String("who"))
	if err != nil {
		t.Fatal(err)
	}
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := caller.Call(ctx, "orbtr.relaytest", relayTestFQN("AtEarly"), payload)
		cancel()
		if _, direct := late.ConnManager().MeshSessionForNode(string(early.NodeID())); direct {
			t.Fatalf("the later joiner holds a session to the earlier joiner, so the call need not have been relayed")
		}
		if err == nil {
			var got wrapperspb.StringValue
			if err := proto.Unmarshal(out, &got); err != nil {
				t.Fatal(err)
			}
			if got.GetValue() != "AtEarly" {
				t.Fatalf("answered %q, want the earlier joiner's AtEarly", got.GetValue())
			}
			return
		}
		lastErr = err
		if errors.Is(err, rpc.ErrRouteDeadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("a call given 5 s through the seed was not answered: %v", lastErr)
}
