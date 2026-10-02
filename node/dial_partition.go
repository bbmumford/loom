/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"net"

	"github.com/ORBTR/aether"
)

// dialPartitionMetaKey is the Capabilities.Tags key under which a node's
// fleet.peer record names the transport partition peers dial it under.
// PeerPublisher writes it (advertisedDialPartition) and RoleTable reads it into
// PeerInfo.DialPartition; both sides must agree on this exact string or no
// record resolves a dial partition. A peer running code that does not know the
// key ignores it.
const dialPartitionMetaKey = "dial_partition"

// transportPartitionOf returns the partition a transport connection's handshake
// bound it to: the ScopeID a noise connection carries from its preamble, set on
// the dialling and on the accepting side alike. AcceptIncomingMeshSession and
// DialAndAcceptMesh pass it to AcceptMeshConnection, which records it on the
// peer. It is "" for a connection that names no partition: a dedicated or
// unpartitioned noise transport, WebSocket, QUIC and TLS.
func transportPartitionOf(conn net.Conn) string {
	if scoped, ok := conn.(aether.PartitionAwareSession); ok {
		return scoped.ScopeID()
	}
	return ""
}

// advertisedDialPartition returns the partition this node's peer record names as
// the one it accepts dials under, and whether it names one. It names one only
// while the shared transport holds exactly one partition: a node holding several
// would otherwise name every partition it serves to every peer that reads
// fleet.peer, and its record, which swarm caps at MaxRecordBytes, would grow
// with that count. PeerPublisher.publishOnce calls it on every publish, so a
// partition registered or removed at runtime is reflected on the next one.
func (rt *Runtime) advertisedDialPartition() (string, bool) {
	if rt == nil || rt.sharedTransport == nil {
		return "", false
	}
	partitions := rt.sharedTransport.Partitions()
	if len(partitions) != 1 {
		return "", false
	}
	return string(partitions[0]), true
}

// dialPartitionFor returns the partition dialNoiseUDP asks the shared transport
// to dial target under when the caller named none, or "" when nothing about the
// target names one. Its sources, in order:
//
//  1. the session: the partition target's most recent noise-UDP session was
//     bound to (peerConn.partition), which a handshake with target proved;
//  2. the target's record: the partition its fleet.peer record names
//     (PeerInfo.DialPartition).
//
// A source's partition is used only while the shared transport still holds its
// keys. It never takes one of this node's configured partitions in place of the
// target's: the responder binds the session to whichever partition the preamble
// names, so a partition chosen here without the target naming it would become
// the session's partition on both ends. With "" the shared transport refuses the
// dial (manager.ErrSharedNoPartitionInCtx).
func (rt *Runtime) dialPartitionFor(target aether.NodeID) string {
	if rt == nil || rt.sharedTransport == nil {
		return ""
	}
	held := func(partition string) bool {
		if partition == "" {
			return false
		}
		_, err := rt.sharedTransport.ResolveKeys(partition)
		return err == nil
	}
	nodeID := aether.NormalizeNodeID(string(target))
	if rt.connMgr != nil {
		if partition := rt.connMgr.sessionPartition(nodeID); held(partition) {
			return partition
		}
	}
	if rt.swarm != nil && rt.swarm.RoleTable != nil {
		if info, ok := rt.swarm.RoleTable.PeerInfo(nodeID); ok && held(info.DialPartition) {
			return info.DialPartition
		}
	}
	return ""
}

// sessionPartition returns the partition nodeID's most recent noise-UDP session
// was bound to, or "" when no such session has registered. dialPartitionFor
// reads it; AcceptMeshConnection writes it under m.mu.
func (m *ConnectionManager) sessionPartition(nodeID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if peer, ok := m.peers[nodeID]; ok {
		return peer.partition
	}
	return ""
}
