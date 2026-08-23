/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// bridgeReachFromSwarm subscribes to the swarm fleet.peer topic and
// translates every inbound PeerRecord into paired LAD records on
// TopicReach + TopicMember + TopicRole, written to the DirectoryCache
// via ApplyLocal (bypasses the signedTopicACL because the swarm engine
// has already verified the upstream signature in plumtrees.go before
// delivery here).
//
// Each emitted lad.Record is re-signed with the bridge node's own
// identity key BEFORE ApplyLocal. The bridge is the re-publishing
// authority: it constructs new wire bytes from the swarm PeerRecord
// (different signable-bytes layout than the upstream swarm signature),
// so the originator's PubKey can't verify what the bridge constructs.
// Wire propagation rides the swarm PeerPublisher path (see
// swarm_integration.go); the bridge-side signature is still required
// because receivers run signedTopicACL → VerifyRecord against the
// bytes the bridge wrote on any LAD-fronted consumer. Without it
// every peer rejects the record AFTER paying lock + verify cost.
//
// The receiving peer's trust decision is "do I trust this bridge's
// pubkey to speak for the target nodeID?" — same identity that signed
// the swarm PeerRecord delivered through the swarm Subscribe path
// (the bridging node is itself an anchor in the role table). The trust
// boundary is the swarm record's signature; the lad signature is a
// transport-format adaptation, not an independent trust claim.
func bridgeReachFromSwarm(rt *Runtime, node swarm.Node, cache *ladcache.DirectoryCache) {
	const fleetPeerTopic = swarm.Topic("fleet.peer")
	_, err := node.Subscribe(fleetPeerTopic, func(r swarm.Record) error {
		nodeID := string(r.NodeID)

		// Feed the split-brain detector from the gossip-ingest path: this is
		// ObservePeer's only caller, so without it the detector observes
		// nothing and detects nothing. Departures drop the peer; live records
		// advance its observed clock.
		if rt.partitionDetector != nil {
			if r.Tombstone {
				rt.partitionDetector.RemovePeer(nodeID)
			} else {
				rt.partitionDetector.ObservePeer(nodeID, r.HLC)
			}
		}

		if r.Tombstone {
			// Owner-emitted or observer-synthesised tombstone. Forward
			// as a propagating LAD tombstone so members/reach drains
			// on every LAD-fronted consumer in lock-step with swarm.
			cache.ApplyLocal(lad.Record{
				Topic:           lad.TopicReach,
				NodeID:          nodeID,
				Tombstone:       true,
				DeletedAt:       time.Now(),
				Timestamp:       time.Now(),
				TombstoneReason: "swarm-bridge",
				HLCTimestamp:    r.HLC,
			})
			cache.ApplyLocal(lad.Record{
				Topic:           lad.TopicMember,
				NodeID:          nodeID,
				Tombstone:       true,
				DeletedAt:       time.Now(),
				Timestamp:       time.Now(),
				TombstoneReason: "swarm-bridge",
				HLCTimestamp:    r.HLC,
			})
			// Also tombstone TopicRole. The live branch below writes
			// all three of Reach/Member/Role, but this departure path only
			// drained Reach+Member — so a departed node's role record lingered
			// until TTL/rebuild, and consumers (isAnchorNode, BestGradeToHandler)
			// kept treating the dead node as an anchor.
			cache.ApplyLocal(lad.Record{
				Topic:           lad.TopicRole,
				NodeID:          nodeID,
				Tombstone:       true,
				DeletedAt:       time.Now(),
				Timestamp:       time.Now(),
				TombstoneReason: "swarm-bridge",
				HLCTimestamp:    r.HLC,
			})
			return nil
		}

		pr := &swarmpb.PeerRecord{}
		if err := proto.Unmarshal(r.Body, pr); err != nil {
			log.Printf("[LAD-BRIDGE] unmarshal PeerRecord nodeID=%s: %v", truncID(nodeID), err)
			return nil
		}

		addrs := reachAddrsFromPB(nodeID, pr.Addresses)

		var (
			region  string
			roles   []string
			service string
			nat     string
		)
		if pr.Capabilities != nil {
			roles = append([]string(nil), pr.Capabilities.Roles...)
			// Capabilities.Tags is a flat []string of "key=value" entries
			// (see role_table.go peerRecordToInfo). service / region /
			// nat_class live in there.
			for _, tag := range pr.Capabilities.Tags {
				eq := strings.IndexByte(tag, '=')
				if eq < 0 {
					continue
				}
				k, v := tag[:eq], tag[eq+1:]
				switch k {
				case "service":
					service = v
				case "region":
					region = v
				case "nat_class":
					nat = v
				case connCountMetaKey:
					// Feed the peer's own open-session count into
					// ConnectionMap. This is the map's only writer, and
					// ConnectionScaler.adjustForGlobalBalance is what reads
					// the resulting IsHotspot verdict to steer connections
					// away from an overloaded node. A malformed or negative
					// value is dropped rather than stored: ConnectionMap
					// averages every entry, so one bad count skews
					// MeshAverage for the whole mesh and moves the hotspot
					// threshold for every peer.
					if n, err := strconv.Atoi(v); err == nil && n >= 0 {
						if rt.connMgr != nil && rt.connMgr.connectionMap != nil {
							rt.connMgr.connectionMap.Update(nodeID, n, 0)
						}
					}
				}
			}
		}

		reach := lad.ReachRecord{
			NodeID:    nodeID,
			Seq:       r.HLC,
			Addresses: addrs,
			Region:    region,
			NATType:   nat,
			UpdatedAt: time.Now().UTC(),
			Metadata: map[string]string{
				"serviceName": service,
				"region":      region,
				"roles":       strings.Join(roles, ","),
			},
		}

		body, err := json.Marshal(reach)
		if err != nil {
			log.Printf("[LAD-BRIDGE] marshal reach nodeID=%s: %v", truncID(nodeID), err)
			return nil
		}

		reachRec := lad.Record{
			Topic:        lad.TopicReach,
			NodeID:       nodeID,
			Seq:          r.HLC,
			Body:         body,
			Timestamp:    time.Now().UTC(),
			HLCTimestamp: r.HLC,
		}
		lad.SignRecord(&reachRec, rt.identity.PrivateKey)
		if err := cache.ApplyLocal(reachRec); err != nil {
			log.Printf("[LAD-BRIDGE] ApplyLocal TopicReach nodeID=%s: %v", truncID(nodeID), err)
		}

		// Write a paired MemberRecord. CacheStats.MemberCount reads from
		// persisted MemberRecord entries (not the Reach-derived synthesised
		// view), so without this lad_members would still report 0 even with
		// a populated reach cache. Members() unifies both sources at read
		// time, so populating persisted entries here is the canonical fix.
		member := lad.MemberRecord{
			NodeID:    nodeID,
			PubKey:    r.PubKey,
			CreatedAt: time.Now().UTC(),
			Attrs: map[string]string{
				"serviceName": service,
				"region":      region,
				"roles":       strings.Join(roles, ","),
			},
		}
		memberBody, err := json.Marshal(member)
		if err != nil {
			log.Printf("[LAD-BRIDGE] marshal member nodeID=%s: %v", truncID(nodeID), err)
			return nil
		}
		memberRec := lad.Record{
			Topic:        lad.TopicMember,
			NodeID:       nodeID,
			Seq:          r.HLC,
			Body:         memberBody,
			Timestamp:    time.Now().UTC(),
			HLCTimestamp: r.HLC,
		}
		lad.SignRecord(&memberRec, rt.identity.PrivateKey)
		if err := cache.ApplyLocal(memberRec); err != nil {
			log.Printf("[LAD-BRIDGE] ApplyLocal TopicMember nodeID=%s: %v", truncID(nodeID), err)
		}

		// Emit a paired TopicRole record so cache.Roles() stays populated
		// for consumers that still read it: isAnchorNode (peer_connections.go),
		// BestGradeToHandler (runtime.go), and /mesh/status Roles passthrough
		// (meshstatus/adapter.go). Without this they would all return empty
		// because swarm PeerRecords are now the sole role source.
		role := lad.RoleRecord{
			NodeID:   nodeID,
			Roles:    roles,
			MaxGrade: int(pr.MaxGrade),
			Updated:  time.Now().UTC(),
		}
		roleBody, err := json.Marshal(role)
		if err != nil {
			log.Printf("[LAD-BRIDGE] marshal role nodeID=%s: %v", truncID(nodeID), err)
			return nil
		}
		roleRec := lad.Record{
			Topic:        lad.TopicRole,
			NodeID:       nodeID,
			Seq:          r.HLC,
			Body:         roleBody,
			Timestamp:    time.Now().UTC(),
			HLCTimestamp: r.HLC,
		}
		lad.SignRecord(&roleRec, rt.identity.PrivateKey)
		if err := cache.ApplyLocal(roleRec); err != nil {
			log.Printf("[LAD-BRIDGE] ApplyLocal TopicRole nodeID=%s: %v", truncID(nodeID), err)
		}
		return nil
	})
	if err != nil {
		log.Printf("[LAD-BRIDGE] Subscribe(fleet.peer) FAILED: %v", err)
	}
}

// addressProtoToReach maps the swarm Address_Transport enum to the
// proto string consumers expect on lad.ReachAddress.
//
// NOISE_UDP MUST map to "udp" (not "noise-udp"). Every downstream
// consumer that filters reach addresses by transport — forwarder.go
// cross-org noise-UDP hairpin filter, peer_connections.go cross-origin
// classification, bestAddress public-UDP filter — keys on a.Proto == "udp".
// Emitting "noise-udp" silently breaks all three filters; cross-org
// UDP-direct dial stops working and traffic falls back to WS-relay.
// reachAddrsFromPB converts a PeerRecord's addresses into reach addresses,
// SKIPPING any whose transport does not map to a dialable proto.
//
// 🛑 THE SKIP IS THE POINT, AND IT WAS MISSING. Address_UNKNOWN is
// the ZERO VALUE of Address_Transport (= 0), so any Address whose Transport
// field is simply unset — a partially-populated message, an older or newer
// producer, a hand-built record — reaches addressProtoToReach's default and
// yields "".
//
// Appending that produced a reach address with an EMPTY Proto: undialable by
// every path (each keys on "udp"/"wss"/…), yet stored in the signed reach
// record, counted among the peer's addresses, and reported as reach data.
// Nothing errors. A peer can therefore appear to have addresses while having
// no usable one.
//
// The sibling doing this correctly already existed: resyncStalePeerAddresses
// skips on `proto == ""`. This path did not, so the two disagreed about the
// same malformed input.
//
// The skip is LOGGED rather than silent, because "the record had entries we
// discarded" is exactly the fact an operator needs when a peer is unreachable
// but its address count looks healthy.
func reachAddrsFromPB(nodeID string, in []*swarmpb.Address) []lad.ReachAddress {
	out := make([]lad.ReachAddress, 0, len(in))
	skipped := 0
	for _, a := range in {
		if a == nil {
			continue
		}
		proto := addressProtoToReach(a.Transport)
		if proto == "" {
			skipped++
			continue
		}
		out = append(out, lad.ReachAddress{
			Host:  a.Host,
			Port:  int(a.Port),
			Proto: proto,
			Scope: a.Scope,
		})
	}
	if skipped > 0 {
		log.Printf("[LAD-BRIDGE] nodeID=%s skipped %d address(es) with unmappable transport "+
			"(Address_UNKNOWN or an enum value this build does not know)", truncID(nodeID), skipped)
	}
	return out
}

func addressProtoToReach(t swarmpb.Address_Transport) string {
	switch t {
	case swarmpb.Address_NOISE_UDP:
		return "udp"
	case swarmpb.Address_WEBSOCKET:
		return "wss"
	case swarmpb.Address_GRPC:
		return "grpc"
	case swarmpb.Address_HTTP:
		return "https"
	default:
		return ""
	}
}
