/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"fmt"
	"log"

	"github.com/ORBTR/aether"
	meshdb "github.com/bbmumford/tursoraft/manager"
)

// startMeshDBTransport builds the mesh database transport when a database
// group declares a raft block, and binds it to the endpoint mesh: the swarm
// member and fleet.peer topics, every session the connection manager
// installs or removes from now on, and every session already live.
//
// Runs in Initialize after InitSwarm and before bootstrapPeers, so no session
// joins unobserved; the manager (openMeshDB) starts later and finds the
// transport running.
func (rt *Runtime) startMeshDBTransport() error {
	dc := rt.cfg.Databases
	if !dc.hasRaft() {
		return nil
	}
	if rt.connMgr == nil || rt.swarm == nil || rt.swarm.Node == nil {
		return fmt.Errorf("mesh database clusters need the endpoint mesh: VL1 and the swarm must be up")
	}
	t := newMeshDBTransport(string(rt.identity.NodeID), rt.cfg.ServiceName, meshDBSeats(dc.Groups), defaultMeshDBTiming())
	if err := t.dir.attachSwarm(rt.swarm.Node); err != nil {
		t.Stop()
		return fmt.Errorf("mesh database transport: subscribe to swarm: %w", err)
	}
	cm := rt.connMgr
	t.connect = func(nodeID string) {
		rt.Go("meshdb.connect", func() { cm.ForceConnect(rt.ctx, nodeID) })
	}
	t.removeHook = cm.AddSessionHook(t.onSession)
	for nodeID, sess := range cm.liveMeshSessions() {
		t.bindSession(nodeID, sess)
	}
	rt.meshDBTransport = t
	log.Printf("[NODE] Mesh database transport started (stream %d, %d cluster keys)", meshDBStreamConfig().StreamID, len(t.allowed))
	return nil
}

// openMeshDB starts the tursoraft manager with the resolved keys, the volume
// root and, when a cluster is declared, the mesh database transport.
func (rt *Runtime) openMeshDB(ctx context.Context) error {
	dc := rt.cfg.Databases
	groups, err := dc.resolvedGroups()
	if err != nil {
		return err
	}
	cfg := meshdb.ManagerConfig{
		Groups:       groups,
		RaftConfig:   meshdb.RaftConfig{LocalDir: dc.LocalDir},
		VolumeDir:    dc.volumeDir(rt.cfg.DataDir),
		SyncInterval: dc.SyncInterval,
	}
	if rt.meshDBTransport != nil {
		cfg.Mesh = rt.meshDBTransport
	}
	m, err := meshdb.NewManager(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create Tursoraft database manager: %w", err)
	}
	rt.meshDB = m
	log.Printf("[NODE] Tursoraft databases initialized (%d groups)", len(groups))
	return nil
}

// shutdownMeshDB stops the manager, then the transport. The manager goes
// first, while the swarm and every session are still up: its shutdown
// withdraws this process's published metadata and hands off leadership over
// the transport, and both must reach the peers.
func (rt *Runtime) shutdownMeshDB() error {
	var err error
	if rt.meshDB != nil {
		if err = rt.meshDB.Shutdown(); err != nil {
			log.Printf("[NODE] Error shutting down MeshDB: %v", err)
		}
	}
	if rt.meshDBTransport != nil {
		rt.meshDBTransport.Stop()
	}
	return err
}

// liveMeshSessions snapshots the dispatch session of every peer that has one
// open.
func (m *ConnectionManager) liveMeshSessions() map[string]aether.Session {
	m.dispatchMu.RLock()
	defer m.dispatchMu.RUnlock()
	out := make(map[string]aether.Session, len(m.meshSessions))
	for id, s := range m.meshSessions {
		if s != nil && !s.IsClosed() {
			out[id] = s
		}
	}
	return out
}
