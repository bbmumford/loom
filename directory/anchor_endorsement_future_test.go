/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/bbmumford/loom/ports"
)

// Under one anchor key, a record dated beyond this node's clock by more than
// MaxIssuedAtSkew outranks no one, whatever source holds it, and the node
// whose own record is so dated is not admitted; a record dated ahead within
// the allowance still outranks an older one.
func TestAuthorizeObserverBoundsIssuedAtByTheReceiversClock(t *testing.T) {
	ctx := context.Background()
	pubA, privA := newTestAnchorKey(t)
	idLive, keyLive, recLive := endorsedTestNode(t, privA)
	idAhead, keyAhead, recAhead := endorsedTestNode(t, privA)
	now := time.Now()
	recLive = reissuedAt(t, privA, idLive, recLive, now.UnixNano())
	p := NewPolicy(PolicyConfig{AnchorKeys: []ed25519.PublicKey{pubA}})

	beyond := reissuedAt(t, privA, idAhead, recAhead, now.Add(time.Hour).UnixNano())
	p.SetAnchorEndorsements(endorsementMap{idLive: recLive, idAhead: beyond})
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idLive, PubKey: keyLive}, ""); err != nil {
		t.Errorf("a record dated an hour ahead outranks the current one: %v", err)
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idAhead, PubKey: keyAhead}, ""); err == nil {
		t.Error("the node whose record is dated an hour ahead is admitted")
	}

	within := reissuedAt(t, privA, idAhead, recAhead, now.Add(MaxIssuedAtSkew-time.Minute).UnixNano())
	p.SetAnchorEndorsements(endorsementMap{idLive: recLive, idAhead: within})
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idAhead, PubKey: keyAhead}, ""); err != nil {
		t.Errorf("a record dated ahead within the allowance is not the freshest: %v", err)
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idLive, PubKey: keyLive}, ""); err == nil {
		t.Error("the older record is admitted beside a fresher one dated within the allowance")
	}
}
