/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"os"
	"strings"
	"testing"
)

// TestWebSocketHandlerEmitsSignedIdentity is a REGRESSION LOCK for the
// AE-P-26 signed-identity invariant on the WS upgrade path. This exact
// defect has now regressed TWICE — first in the library's /mesh/ws handler
// (fixed by aether v0.0.110 AcceptHeaders + library v0.0.440), then
// re-shipped in loom v0.0.1 because mesh/node was extracted BEFORE that fix
// (forward-ported by @P in loom v0.0.2, a5c4966). The failure mode is "the
// fix does not travel with the code" across a migration/re-extraction, so a
// prose warning is not enough — this test is the version of the warning
// that survives a rotation, a re-extraction, and an agent handover
// (#R-480: the rule goes in the code, at the site; #S-18).
//
// The invariant: WebSocketHandler MUST emit the aether transport's signed
// identity triple (NodeID/PubKey/Signature) in the 101 response via
// wsTr.AcceptHeaders through ws.HTTPUpgrader. A bare ws.UpgradeHTTP omits
// them, the aether WS dialer then closes the accepted socket ("server did
// not present a signed identity"), gossip starves, no 6PN UDP addresses
// propagate, and the ws->noise-UDP upgrade never fires — the noise-UDP
// regression.
//
// This is a source guard rather than a live upgrade because the emit is a
// build-time property of the handler: the wire-level assertion belongs to
// aether's own AcceptHeaders tests, while what regressed here is the call
// site being reverted to the bare upgrader across an extraction.
func TestWebSocketHandlerEmitsSignedIdentity(t *testing.T) {
	src, err := os.ReadFile("runtime.go")
	if err != nil {
		t.Fatalf("read runtime.go: %v", err)
	}
	code := string(src)

	// (1) The bare upgrader is BANNED anywhere in this file — that is the
	// exact pattern that has regressed twice. If a future edit or
	// re-extraction reintroduces `ws.UpgradeHTTP(`, this fails loudly.
	if n := strings.Count(code, "ws.UpgradeHTTP("); n != 0 {
		t.Fatalf("bare ws.UpgradeHTTP( found %d time(s) in runtime.go — it omits the "+
			"AE-P-26 signed identity headers and re-breaks noise-UDP (regressed twice). "+
			"Use ws.HTTPUpgrader{Header: rt.connMgr.wsTr.AcceptHeaders(r)}.Upgrade instead.", n)
	}

	// (2) The WebSocketHandler body MUST emit AcceptHeaders via HTTPUpgrader.
	body := funcBody(code, "func (rt *Runtime) WebSocketHandler()")
	if body == "" {
		t.Fatal("WebSocketHandler not found in runtime.go — the WS upgrade guard cannot verify the emit")
	}
	if !strings.Contains(body, "AcceptHeaders") {
		t.Fatal("WebSocketHandler does not call AcceptHeaders — the signed identity triple " +
			"(NodeID/PubKey/Signature) is not emitted in the 101 response; the aether WS dialer " +
			"will reject the peer and noise-UDP will regress")
	}
	if !strings.Contains(body, "ws.HTTPUpgrader") {
		t.Fatal("WebSocketHandler does not use ws.HTTPUpgrader — AcceptHeaders must be passed as " +
			"the upgrade response Header; a bare upgrader drops them")
	}
}

// funcBody returns the source of the function whose signature starts with
// sig, from the signature to its matching closing brace at column 0.
// Adequate for top-level funcs formatted by gofmt (closing brace in col 0).
func funcBody(code, sig string) string {
	i := strings.Index(code, sig)
	if i < 0 {
		return ""
	}
	rest := code[i:]
	// End at the next top-level "\n}\n" (gofmt closes a top-level func with a
	// brace in column 0).
	if end := strings.Index(rest, "\n}\n"); end >= 0 {
		return rest[:end]
	}
	return rest
}
