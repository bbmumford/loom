/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"errors"
	"testing"

	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// A derived registry judges under its parent's presence policy, authorization
// policy and refusal recorder, read at each call: a recorder registered on the
// parent after the derivation records the derived registry's refusals.
func TestADerivedRegistryJudgesUnderItsParentsPoliciesAndRecorder(t *testing.T) {
	parent := NewRegistry()
	derived := parent.Derive()
	parent.SetPresencePolicy(refPolicy())
	authz := scope.NewPolicy(nil)
	parent.SetAuthzPolicy(authz)
	rec := &countingRecorder{}
	parent.SetRefusalRecorder(rec)

	if derived.PresencePolicy() == nil || derived.AuthzPolicy() != authz || derived.RefusalRecorder() != rec {
		t.Fatalf("derived registry reads presence %v, authz %p, recorder %v; want the parent's", derived.PresencePolicy(), derived.AuthzPolicy(), derived.RefusalRecorder())
	}
	var calls int
	h := withinPartitionHandler(&calls)
	if err := derived.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, ok := parent.Get(h.FQN()); ok {
		t.Fatalf("a handler registered on the derived registry is on the parent")
	}
	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", "user-1"))
	if _, err := derived.Dispatch(ctx, h.FQN(), nil); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("Dispatch = %v, want ErrScopeDenied", err)
	}
	if len(rec.got) != 1 || rec.got[0].Operation != h.FQN() || calls != 0 {
		t.Fatalf("recorded %+v (calls %d), want the parent's recorder to hold one refusal of %s", rec.got, calls, h.FQN())
	}

	rec.fail = errors.New("audit host down")
	if _, err := derived.Dispatch(ctx, h.FQN(), nil); !errors.Is(err, ErrRefusalUnrecorded) || !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("Dispatch with a failing parent recorder = %v, want the denial joined with ErrRefusalUnrecorded", err)
	}
}

// A Set call on the derived registry ends the inheritance of that one policy or
// the recorder, a nil one included, and leaves the others read from the parent.
func TestASetOnTheDerivedRegistryEndsThatInheritanceAlone(t *testing.T) {
	parent := NewRegistry()
	parent.SetPresencePolicy(refPolicy())
	parent.SetAuthzPolicy(scope.NewPolicy(nil))
	parentRec := &countingRecorder{}
	parent.SetRefusalRecorder(parentRec)

	derived := parent.Derive()
	derived.SetAuthzPolicy(nil)
	if derived.AuthzPolicy() != nil {
		t.Fatalf("AuthzPolicy after SetAuthzPolicy(nil) = %p, want nil", derived.AuthzPolicy())
	}
	if derived.PresencePolicy() == nil || derived.RefusalRecorder() != parentRec {
		t.Fatalf("setting authz ended the presence policy's or the recorder's inheritance")
	}
	own := &countingRecorder{}
	derived.SetRefusalRecorder(own)
	derived.SetPresencePolicy(nil)
	if derived.RefusalRecorder() != own || derived.PresencePolicy() != nil {
		t.Fatalf("derived registry reads recorder %v and presence %v, want its own", derived.RefusalRecorder(), derived.PresencePolicy())
	}
	if parent.RefusalRecorder() != parentRec || parent.PresencePolicy() == nil {
		t.Fatalf("a Set call on the derived registry changed the parent")
	}
}

// heldCheckPoint is a CheckPoint held outside a Registry, as a server holding
// the three for its own dispatch holds them.
type heldCheckPoint struct {
	presence scope.PresencePolicy
	authz    *scope.Policy
	rec      scope.RefusalRecorder
}

func (c heldCheckPoint) PresencePolicy() scope.PresencePolicy   { return c.presence }
func (c heldCheckPoint) AuthzPolicy() *scope.Policy             { return c.authz }
func (c heldCheckPoint) RefusalRecorder() scope.RefusalRecorder { return c.rec }

// EnforceAt judges under the check point's policies and records each refusal
// through its recorder, as Registry.Enforce does; a refusal the recorder cannot
// record stands, joined with ErrRefusalUnrecorded; an admitted call records
// nothing; and a nil check point refuses.
func TestEnforceAtRecordsARefusalThroughTheCheckPoint(t *testing.T) {
	var calls int
	h := withinPartitionHandler(&calls)
	rec := &countingRecorder{}
	cp := heldCheckPoint{presence: refPolicy(), rec: rec}

	refused := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", "user-1"))
	if err := EnforceAt(refused, cp, &h); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("EnforceAt = %v, want ErrScopeDenied", err)
	}
	if len(rec.got) != 1 || rec.got[0].Operation != h.FQN() || rec.got[0].Scope != ScopeWithinPartition || rec.got[0].Caller.Subject != "user-1" {
		t.Fatalf("recorded %+v, want one refusal of %s for user-1", rec.got, h.FQN())
	}
	admitted := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "org-1", "user-1"))
	if err := EnforceAt(admitted, cp, &h); err != nil || len(rec.got) != 1 {
		t.Fatalf("admitted EnforceAt = %v with %d refusals recorded, want nil and 1", err, len(rec.got))
	}

	rec.fail = errors.New("audit host down")
	if err := EnforceAt(refused, cp, &h); !errors.Is(err, ErrRefusalUnrecorded) || !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("EnforceAt with a failing recorder = %v, want the denial joined with ErrRefusalUnrecorded", err)
	}
	if err := EnforceAt(admitted, nil, &h); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("EnforceAt with no check point = %v, want ErrScopeDenied", err)
	}
}
