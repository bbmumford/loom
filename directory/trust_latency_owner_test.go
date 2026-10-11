/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"testing"

	"github.com/bbmumford/loom/ports"
)

// A node publishes latency samples only on the topic its own id forms under
// LatencyTopicPrefix. SwarmDirectory reads the observing node from the topic
// and replaces or, on a tombstone, deletes that node's samples, so a node
// admitted on another node's topic could rewrite or erase what that node
// measured. The other three self-owned rules name one topic each and are
// unaffected, and a rule a policy supplies for the namespace still cannot
// close a node's own topic.
func TestANodePublishesLatencyOnlyUnderItsOwnID(t *testing.T) {
	ctx := context.Background()
	node, _ := trustPrincipal(t, "")
	other, _ := trustPrincipal(t, "")

	for name, p := range map[string]*Policy{
		"the baseline": NewPolicy(BaselinePolicyConfig()),
		"with a supplied rule on the namespace": func() *Policy {
			cfg := BaselinePolicyConfig()
			cfg.PublishPrefixes = map[string][]ports.NodeID{LatencyTopicPrefix: {other.NodeID}}
			return NewPolicy(cfg)
		}(),
	} {
		if err := p.AuthorizePublish(ctx, node, LatencyTopic(node.NodeID)); err != nil {
			t.Errorf("%s: a bound node was refused its own latency topic: %v", name, err)
		}
		for _, topic := range []ports.Topic{
			LatencyTopic(other.NodeID),
			LatencyTopic(node.NodeID) + ".x",
			ports.Topic(LatencyTopicPrefix),
			ports.Topic(LatencyTopicPrefix + "unrelated"),
		} {
			if err := p.AuthorizePublish(ctx, node, topic); err == nil {
				t.Errorf("%s: a bound node was admitted on %q, a latency topic that is not its own", name, topic)
			}
		}
		for _, topic := range []ports.Topic{FleetPeerTopic, FleetRouteTopic, MeshDBMemberTopic} {
			if err := p.AuthorizePublish(ctx, node, topic); err != nil {
				t.Errorf("%s: a bound node was refused its own %q record: %v", name, topic, err)
			}
		}
	}
}
