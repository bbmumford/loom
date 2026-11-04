/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/loom/ports"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

func TestHandlerRequirementsSurviveTheTag(t *testing.T) {
	reqs := []string{"open", "open", "capability:orbtr:audit.write", "nexus", "nexus", "open", "scope:a,b;c=d x"}
	value := EncodeHandlerRequirements(reqs)
	if got := DecodeHandlerRequirements(value, len(reqs)); !reflect.DeepEqual(got, reqs) {
		t.Fatalf("decoded %v from %q, want %v", got, value, reqs)
	}
	tags := []string{"service=x", HandlerRequirementsTag + "=" + value}
	if got := HandlerRequirements(tags, len(reqs)); !reflect.DeepEqual(got, reqs) {
		t.Fatalf("found %v in tags, want %v", got, reqs)
	}
	if got := HandlerRequirements([]string{"service=x"}, len(reqs)); got != nil {
		t.Fatalf("a record with no tag named %v", got)
	}
}

// A value that does not cover exactly the listed operations names nothing, so a
// mangled or stale record reads as a node that declares no requirement rather than
// as requirements attached to the wrong operations.
func TestAHandlerRequirementsValueThatDoesNotFitNamesNothing(t *testing.T) {
	good := EncodeHandlerRequirements([]string{"open", "nexus"})
	for name, c := range map[string]struct {
		value    string
		handlers int
	}{
		"fewer listed":    {good, 1},
		"more listed":     {good, 3},
		"no runs":         {"open", 1},
		"index past end":  {"open;1x1", 1},
		"negative index":  {"open;-1x1", 1},
		"zero count":      {"open;0x0", 1},
		"bad count":       {"open;0xz", 1},
		"bad escape":      {"%zz;0x1", 1},
		"run without x":   {"open;01", 1},
		"none listed":     {"open;0x1", 0},
		"overflowed runs": {"open;0x9223372036854775807", 2},
	} {
		if got := DecodeHandlerRequirements(c.value, c.handlers); got != nil {
			t.Errorf("%s: DecodeHandlerRequirements(%q, %d) = %v, want nil", name, c.value, c.handlers, got)
		}
	}
}

// The tag stays small beside the FQNs it describes, because they share one record
// under swarm's size cap: operations of one domain sit together and mostly ask the
// same thing.
func TestHandlerRequirementsCostRunsNotOperations(t *testing.T) {
	var reqs []string
	for domain := 0; domain < 25; domain++ {
		for op := 0; op < 15; op++ {
			reqs = append(reqs, fmt.Sprintf("capability:orbtr.platform:domain%02d.read", domain))
		}
	}
	value := EncodeHandlerRequirements(reqs)
	if len(value) > 1500 {
		t.Errorf("375 operations over 25 requirements take %d bytes on the record, want at most 1500", len(value))
	}
}

// The LAD side of the port carries what each advert requires, read from the role
// record the bridge writes.
func TestLADHandlersByNameCarriesTheRequirement(t *testing.T) {
	cache := ladcache.NewDirectoryCache()
	body, err := json.Marshal(lad.RoleRecord{
		NodeID: "node-a", Roles: []string{"orbtr.audit"}, Updated: time.Now(),
		Handlers: []lad.HandlerMetadata{{Name: "orbtr.audit.Write", RequiredScopes: []string{"nexus"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Apply(lad.Record{Topic: lad.TopicRole, NodeID: "node-a", Body: body, Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	dir, err := NewLADDirectory(cache)
	if err != nil {
		t.Fatal(err)
	}
	got, err := dir.HandlersByName(context.Background(), "", "orbtr.audit.Write")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0].RequiredScopes, []string{"nexus"}) {
		t.Fatalf("adverts = %+v, want one naming nexus", got)
	}
}

// The swarm side reads the same tag, so the two sides of the port state one thing
// about one record.
func TestSwarmHandlersByNameCarriesTheRequirement(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)
	d := newTestDirectory(t, nil)
	rec := peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "auth-svc")

	var pr swarmpb.PeerRecord
	if err := proto.Unmarshal(rec.Body, &pr); err != nil {
		t.Fatal(err)
	}
	reqs := make([]string, len(pr.RpcHandlers))
	for i := range reqs {
		reqs[i] = "capability:orbtr:auth.read"
	}
	pr.Capabilities.Tags = append(pr.Capabilities.Tags, HandlerRequirementsTag+"="+EncodeHandlerRequirements(reqs))
	body, err := proto.Marshal(&pr)
	if err != nil {
		t.Fatal(err)
	}
	rec.Body = body
	if err := d.Ingest(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := d.HandlersByName(ctx, testPartitionID, pr.RpcHandlers[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0].RequiredScopes, []string{"capability:orbtr:auth.read"}) {
		t.Fatalf("adverts = %+v, want the requirement the tag names", got)
	}
}

type fixedAdverts struct{ adverts []ports.HandlerAdvert }

func (f fixedAdverts) HandlerAdverts(string) []ports.HandlerAdvert { return f.adverts }
func (f fixedAdverts) HandlerNames() []string                      { return []string{"orbtr.audit.Write"} }

// The source answers for the unpartitioned bucket alone, the one role adverts sit
// in: a partition is a literal bucket key, and adverts of another bucket do not read
// as this one's.
func TestAnAdvertSourceAnswersForTheUnpartitionedBucketAlone(t *testing.T) {
	ctx := context.Background()
	dir, err := NewLADDirectory(ladcache.NewDirectoryCache())
	if err != nil {
		t.Fatal(err)
	}
	dir.SetHandlerAdverts(fixedAdverts{adverts: []ports.HandlerAdvert{{Name: "orbtr.audit.Write", NodeID: "n1"}}})

	if got, _ := dir.HandlersByName(ctx, "", "orbtr.audit.Write"); len(got) != 1 {
		t.Fatalf("unpartitioned adverts = %+v, want the source's", got)
	}
	if got, _ := dir.HandlerFQNs(ctx, ""); !reflect.DeepEqual(got, []string{"orbtr.audit.Write"}) {
		t.Fatalf("unpartitioned names = %v, want the source's", got)
	}
	if got, _ := dir.HandlersByName(ctx, "p1", "orbtr.audit.Write"); len(got) != 0 {
		t.Fatalf("partition p1 adverts = %+v, want none", got)
	}
	if got, _ := dir.HandlerFQNs(ctx, "p1"); len(got) != 0 {
		t.Fatalf("partition p1 names = %v, want none", got)
	}
}

// A node both the source and a role record name is counted once, as the source has it.
func TestASourceAdvertStandsForANodeARoleRecordAlsoNames(t *testing.T) {
	cache := ladcache.NewDirectoryCache()
	body, err := json.Marshal(lad.RoleRecord{
		NodeID: "n1", Roles: []string{"orbtr.audit"}, Updated: time.Now(),
		Handlers: []lad.HandlerMetadata{{Name: "orbtr.audit.Write", RequiredScopes: []string{"open"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Apply(lad.Record{Topic: lad.TopicRole, NodeID: "n1", Body: body, Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	dir, err := NewLADDirectory(cache)
	if err != nil {
		t.Fatal(err)
	}
	dir.SetHandlerAdverts(fixedAdverts{adverts: []ports.HandlerAdvert{
		{Name: "orbtr.audit.Write", NodeID: "n1", RequiredScopes: []string{"nexus"}},
	}})
	got, err := dir.HandlersByName(context.Background(), "", "orbtr.audit.Write")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0].RequiredScopes, []string{"nexus"}) {
		t.Fatalf("adverts = %+v, want the source's one", got)
	}
}
