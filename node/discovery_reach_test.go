// Copyright (c) 2026 Brandon Miller-Mumford. Licensed under the MIT License; see LICENSE.

package node

import (
	"context"
	"reflect"
	"testing"

	"github.com/bbmumford/reach/discovery"
)

// TestRuntimeDiscoveryLayersAreReachDiscoverers holds the runtime's discovery
// seam to reach's discovery package: AddDiscoverer takes reach's Discoverer,
// so a layer built from reach (the endpoint spine's DNS SRV root, a seed
// list) is what the dial loop's multi-layer composer runs. The layer is added
// through reflection so the test compiles whatever type the seam takes, and
// fails on the type rather than at build time.
func TestRuntimeDiscoveryLayersAreReachDiscoverers(t *testing.T) {
	m, ok := reflect.TypeOf((*Runtime)(nil)).MethodByName("AddDiscoverer")
	if !ok {
		t.Fatal("Runtime has no AddDiscoverer")
	}
	if m.Type.NumIn() != 2 {
		t.Fatalf("AddDiscoverer takes %d arguments besides the receiver, want 1", m.Type.NumIn()-1)
	}
	got, want := m.Type.In(1), reflect.TypeOf((*discovery.Discoverer)(nil)).Elem()
	if got != want {
		t.Fatalf("AddDiscoverer takes %s from %q, want %s from %q", got, got.PkgPath(), want, want.PkgPath())
	}

	rt := &Runtime{}
	seed := &discovery.SeedDiscoverer{Seeds: []string{"seed.example:7946"}}
	m.Func.Call([]reflect.Value{reflect.ValueOf(rt), reflect.ValueOf(seed)})
	if len(rt.discoverers) != 1 || any(rt.discoverers[0]) != any(seed) {
		t.Fatalf("AddDiscoverer kept %v, want the one seed layer it was given", rt.discoverers)
	}
	peers, err := rt.discoverers[0].Discover(context.Background())
	if err != nil || len(peers) != 1 || peers[0].String() != "seed.example:7946" {
		t.Fatalf("the seed layer discovered %v, %v; want seed.example:7946 and no error", peers, err)
	}
}
