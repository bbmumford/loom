/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bbmumford/whisper"
)

// These tests cover the wait a runtime's stop sequence makes for its tracker's
// last watermark save: Runtime.persistDeltaWatermarks, the deltaWatermarkStore
// the tracker saves through, and the goroutine that waits on the store.

// gatedPersistence is a watermark backend whose Save reports that it has
// started and then blocks until release is closed, so a test can hold the
// tracker's last save open and see whether the runtime's stop sequence waits
// for it.
type gatedPersistence struct {
	started chan struct{}
	release chan struct{}

	mu    sync.Mutex
	saves [][]whisper.PersistedPeerState
}

func (p *gatedPersistence) Save(states []whisper.PersistedPeerState) error {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-p.release
	p.mu.Lock()
	defer p.mu.Unlock()
	p.saves = append(p.saves, states)
	return nil
}

func (p *gatedPersistence) Load() ([]whisper.PersistedPeerState, error) { return nil, nil }

func (p *gatedPersistence) written() [][]whisper.PersistedPeerState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]whisper.PersistedPeerState(nil), p.saves...)
}

// Stopping a runtime waits for its tracker's last watermark save. The stop
// sequence Shutdown runs, cancel then wait for the goroutines Runtime.Go
// enrolled, does not return while that save is still writing; the save
// carries the watermarks; and a save arriving after the sequence returned
// reaches no backend, so nothing is written under a data directory the next
// generation has started to read.
func TestStoppingARuntimeWaitsForItsLastWatermarkSave(t *testing.T) {
	rt := deltaRuntime(t, t.TempDir())
	rt.deltaTracker = whisper.NewDeltaTracker()
	backend := &gatedPersistence{started: make(chan struct{}, 1), release: make(chan struct{})}
	store := rt.persistDeltaWatermarks(backend)
	rt.deltaTracker.UpdateWatermark("peer-a", time.Now())

	rt.cancel()
	stopped := make(chan struct{})
	go func() {
		rt.wg.Wait()
		close(stopped)
	}()
	select {
	case <-backend.started:
	case <-time.After(3 * time.Second):
		close(backend.release)
		t.Fatal("the tracker made no save when the runtime's context ended")
	}
	select {
	case <-stopped:
		close(backend.release)
		t.Fatal("the stop sequence returned while the tracker's last save was still writing")
	case <-time.After(100 * time.Millisecond):
	}
	close(backend.release)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("the stop sequence did not return once the last save had finished")
	}

	saves := backend.written()
	if len(saves) != 1 || len(saves[0]) != 1 || saves[0][0].PeerID != "peer-a" {
		t.Fatalf("the last save wrote %+v, want one save holding peer-a's watermark", saves)
	}
	if err := store.Save([]whisper.PersistedPeerState{{PeerID: "late"}}); err != nil {
		t.Fatalf("a save after the stop: %v", err)
	}
	if got := len(backend.written()); got != 1 {
		t.Fatalf("%d saves reached the backend, want 1: a save after the stop sequence returned was written", got)
	}
}

// A deltaWatermarkStore passes each save to its backend while its runtime's
// context is live, writes the first save after the context ends and reports
// it through waitLastSave, and writes nothing after that.
func TestTheWatermarkStoreWritesOneSaveAfterItsContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend, _ := newTestPersistence(t)
	store := &deltaWatermarkStore{backend: backend, ended: ctx.Done(), last: make(chan struct{})}
	stored := func() string {
		t.Helper()
		states, err := store.Load()
		if err != nil || len(states) != 1 {
			t.Fatalf("Load = %+v, %v; want one state", states, err)
		}
		return states[0].PeerID
	}
	save := func(peer string) {
		t.Helper()
		if err := store.Save([]whisper.PersistedPeerState{{PeerID: peer}}); err != nil {
			t.Fatalf("Save %s: %v", peer, err)
		}
	}

	for _, peer := range []string{"while-live-1", "while-live-2"} {
		save(peer)
		if got := stored(); got != peer {
			t.Fatalf("a save while the context is live left %q stored, want %q", got, peer)
		}
	}

	cancel()
	waited := make(chan struct{})
	go func() {
		store.waitLastSave()
		close(waited)
	}()
	select {
	case <-waited:
		t.Fatal("waitLastSave returned before any save followed the context's end")
	case <-time.After(50 * time.Millisecond):
	}
	save("last")
	select {
	case <-waited:
	case <-time.After(3 * time.Second):
		t.Fatal("waitLastSave did not return after the save that followed the context's end")
	}
	if got := stored(); got != "last" {
		t.Fatalf("the save after the context ended left %q stored, want it written", got)
	}
	save("late")
	if got := stored(); got != "last" {
		t.Fatalf("a second save after the context ended was written: %q is stored", got)
	}
}
