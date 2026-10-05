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

// TestWebSocketHandlerEmitsSignedIdentity guards the signed-identity
// invariant on the WS upgrade path. Swapping the upgrader at the call site
// for the bare one compiles and leaves the handler's other tests passing,
// and a copy of this handler into another module carries whichever call it
// was copied with, so the rule is held as an assertion over runtime.go's
// source rather than only as a comment at the site.
//
// The invariant: WebSocketHandler MUST emit the aether transport's signed
// identity triple (NodeID/PubKey/Signature) in the 101 response via
// wsTr.AcceptHeaders through ws.HTTPUpgrader. A bare ws.UpgradeHTTP omits
// them, the aether WS dialer then closes the accepted socket ("server did
// not present a signed identity"), gossip starves, no 6PN UDP addresses
// propagate, and the ws->noise-UDP upgrade never fires.
//
// This is a source guard rather than a live upgrade because the emit is a
// build-time property of the handler: the wire-level assertion belongs to
// aether's own AcceptHeaders tests, while what this guards is the call
// site, which an edit can turn back into the bare upgrader.
func TestWebSocketHandlerEmitsSignedIdentity(t *testing.T) {
	src, err := os.ReadFile("runtime.go")
	if err != nil {
		t.Fatalf("read runtime.go: %v", err)
	}
	code := string(src)

	// (1) The bare upgrader is BANNED anywhere in this file: it omits the
	// signed identity headers. An edit that introduces `ws.UpgradeHTTP(`
	// fails here.
	if n := strings.Count(code, "ws.UpgradeHTTP("); n != 0 {
		t.Fatalf("bare ws.UpgradeHTTP( found %d time(s) in runtime.go — it omits the "+
			"signed identity headers, so the aether WS dialer closes the socket and noise-UDP never engages. "+
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
			"will reject the peer and the ws->noise-UDP upgrade never fires")
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
