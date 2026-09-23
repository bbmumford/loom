/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"log"
	"maps"
	"sync"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// topicMeshDBMember carries what each process publishes under a mesh
// database cluster key: one keyed slot per (node, cluster key), whose body is
// the JSON of the metadata map tursoraft passed to Publish. Every record is
// signed by its node's key like every swarm record, and a slot is withdrawn
// with a keyed tombstone. directory.SelfOwnedPublishPrefixes lists it, because
// the enforcing swarm trust gate refuses a topic no rule names.
const topicMeshDBMember = swarm.Topic(directory.MeshDBMemberTopic)

// meshDBDirectory is the membership view the mesh database transport serves
// tursoraft from: the metadata each node publishes per cluster key, and the
// service each node runs.
//
// Consumes two swarm topics: topicMeshDBMember for metadata, and fleet.peer
// for the service a node names in its signed PeerRecord ("service=" tag,
// PeerPublisher). Consumed by MeshDBTransport, which filters the members by
// live sessions and authorizes inbound streams on the service.
//
// This process's own metadata is authoritative locally: Publish records it
// before gossip does, and a record under this NodeID that this process did not
// publish is a previous process's leftover, which is withdrawn so peers stop
// counting a member that no longer holds that seat.
type meshDBDirectory struct {
	self string

	// publishFn and withdrawFn put this process's slot on the mesh; nil in
	// tests that wire directories together by hand.
	publishFn  func(key string, body []byte) error
	withdrawFn func(key string) error
	// onChange runs after every change, with no lock held.
	onChange func()

	// slotMu orders every publish and withdrawal of this process's own
	// slots. onMemberRecord withdraws a leftover slot on a goroutine of its
	// own; without the order, a withdrawal decided before tursoraft's first
	// Publish of that key could reach the mesh after it, and every peer
	// would then hold a tombstone for a slot this process believes is
	// published and republishes only when its metadata changes.
	slotMu sync.Mutex

	mu       sync.Mutex
	meta     map[string]map[string]map[string]string // key -> node -> metadata
	services map[string]string                       // node -> service
	own      map[string]bool                         // keys this process has published and not withdrawn

	unsubs []swarm.Unsubscribe
	wg     sync.WaitGroup
}

func newMeshDBDirectory(self string, onChange func()) *meshDBDirectory {
	return &meshDBDirectory{
		self:     self,
		onChange: onChange,
		meta:     map[string]map[string]map[string]string{},
		services: map[string]string{},
		own:      map[string]bool{},
	}
}

// attachSwarm subscribes the directory to node's member and fleet.peer topics
// and publishes through it.
func (d *meshDBDirectory) attachSwarm(node swarm.Node) error {
	d.publishFn = func(key string, body []byte) error { return node.PublishKeyed(topicMeshDBMember, key, body) }
	d.withdrawFn = func(key string) error { return node.PublishKeyedTombstone(topicMeshDBMember, key) }
	unsubMember, err := node.Subscribe(topicMeshDBMember, d.onMemberRecord)
	if err != nil {
		return err
	}
	unsubPeer, err := node.Subscribe(topicFleetPeer, d.onPeerRecord)
	if err != nil {
		unsubMember()
		return err
	}
	d.unsubs = []swarm.Unsubscribe{unsubMember, unsubPeer}
	return nil
}

// close ends the subscriptions and waits for any withdrawal in flight.
func (d *meshDBDirectory) close() {
	for _, u := range d.unsubs {
		u()
	}
	d.unsubs = nil
	d.wg.Wait()
}

// publish replaces this process's metadata under key; nil withdraws it.
func (d *meshDBDirectory) publish(key string, meta map[string]string) error {
	d.slotMu.Lock()
	defer d.slotMu.Unlock()
	d.mu.Lock()
	if meta == nil {
		delete(d.own, key)
		d.setLocked(key, d.self, nil)
	} else {
		d.own[key] = true
		d.setLocked(key, d.self, maps.Clone(meta))
	}
	d.mu.Unlock()
	var err error
	switch {
	case meta == nil && d.withdrawFn != nil:
		err = d.withdrawFn(key)
	case meta != nil && d.publishFn != nil:
		var body []byte
		if body, err = json.Marshal(meta); err == nil {
			err = d.publishFn(key, body)
		}
	}
	d.changed()
	return err
}

// records returns every node's metadata under key, this process's included.
func (d *meshDBDirectory) records(key string) map[string]map[string]string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]map[string]string, len(d.meta[key]))
	for node, m := range d.meta[key] {
		out[node] = maps.Clone(m)
	}
	return out
}

// published reports whether node publishes under key.
func (d *meshDBDirectory) published(key, node string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.meta[key][node]
	return ok
}

// publishesAny reports whether node publishes under any cluster key: the
// condition for this process to hold a channel to it.
func (d *meshDBDirectory) publishesAny(node string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, nodes := range d.meta {
		if _, ok := nodes[node]; ok {
			return true
		}
	}
	return false
}

// service returns the service node names in its PeerRecord, "" when unknown.
func (d *meshDBDirectory) service(node string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.services[node]
}

func (d *meshDBDirectory) setLocked(key, node string, meta map[string]string) {
	if meta == nil {
		delete(d.meta[key], node)
		if len(d.meta[key]) == 0 {
			delete(d.meta, key)
		}
		return
	}
	if d.meta[key] == nil {
		d.meta[key] = map[string]map[string]string{}
	}
	d.meta[key][node] = meta
}

// setService records node's service; "" forgets it.
func (d *meshDBDirectory) setService(node, service string) {
	d.mu.Lock()
	if d.services[node] == service {
		d.mu.Unlock()
		return
	}
	if service == "" {
		delete(d.services, node)
	} else {
		d.services[node] = service
	}
	d.mu.Unlock()
	d.changed()
}

// ingest applies another node's slot; nil meta removes it.
func (d *meshDBDirectory) ingest(key, node string, meta map[string]string) {
	d.mu.Lock()
	d.setLocked(key, node, meta)
	d.mu.Unlock()
	d.changed()
}

func (d *meshDBDirectory) changed() {
	if d.onChange != nil {
		d.onChange()
	}
}

// onMemberRecord is the topicMeshDBMember subscriber. It must not block: the
// withdrawal of a leftover self record runs on its own goroutine.
func (d *meshDBDirectory) onMemberRecord(r swarm.Record) error {
	node := string(r.NodeID)
	if r.Key == "" || r.IsObserverAttestation() {
		return nil
	}
	if node == d.self {
		d.mu.Lock()
		leftover := !r.Tombstone && !d.own[r.Key]
		d.mu.Unlock()
		if leftover && d.withdrawFn != nil {
			d.wg.Add(1)
			go func(key string) {
				defer d.wg.Done()
				d.slotMu.Lock()
				defer d.slotMu.Unlock()
				d.mu.Lock()
				mine := d.own[key]
				d.mu.Unlock()
				if mine {
					return
				}
				if err := d.withdrawFn(key); err != nil {
					log.Printf("[MESHDB] withdraw a previous process's slot %s: %v", key, err)
				}
			}(r.Key)
		}
		return nil
	}
	if r.Tombstone {
		d.ingest(r.Key, node, nil)
		return nil
	}
	var meta map[string]string
	if err := json.Unmarshal(r.Body, &meta); err != nil || meta == nil {
		log.Printf("[MESHDB] dropped a malformed member slot %s from %s: %v", r.Key, truncID(node), err)
		return nil
	}
	d.ingest(r.Key, node, meta)
	return nil
}

// onPeerRecord is the fleet.peer subscriber: it keeps each node's service.
func (d *meshDBDirectory) onPeerRecord(r swarm.Record) error {
	node := string(r.NodeID)
	if r.IsObserverAttestation() || node == d.self {
		return nil
	}
	if r.Tombstone {
		d.setService(node, "")
		return nil
	}
	pr := &swarmpb.PeerRecord{}
	if err := proto.Unmarshal(r.Body, pr); err != nil {
		return nil
	}
	d.setService(node, peerRecordToInfo(pr, node).ServiceName)
	return nil
}
