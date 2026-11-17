/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/bbmumford/loom/pkg/rpc"
)

// errorEvent reads the data of the one "error" event in an event-stream body.
func errorEvent(t *testing.T, body string) map[string]any {
	t.Helper()
	const marker = "event: error\ndata: "
	at := strings.Index(body, marker)
	if at < 0 {
		t.Fatalf("no error event in %q", body)
	}
	line := body[at+len(marker):]
	line = line[:strings.Index(line, "\n")]
	var data map[string]any
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		t.Fatalf("error event data %q: %v", line, err)
	}
	return data
}

// A stream that fails after its first frame answers one error event whose data
// carries what a unary call failing with the same error is answered: its message
// (error), its HTTP status (status) and a handler refusal's wire code (code, as
// the unary body writes it beside a 4xx), beside the refusal's mesh kind (kind),
// so a client maps a stream's failure as it maps a unary refusal. A failure that
// is no refusal carries its 5xx status, its status text, no code and no kind.
func TestAStreamsErrorEventCarriesTheUnaryStatusAndTheRefusalKind(t *testing.T) {
	for _, c := range []struct {
		name       string
		fail       error
		wantStatus int
		wantCode   string
		wantKind   string
		wantText   string
	}{
		{"a conflict", fmt.Errorf("stale revision: %w", rpc.ErrConflict), http.StatusConflict, "", "conflict", "stale revision"},
		{"a denial", fmt.Errorf("not yours: %w", rpc.ErrScopeDenied), http.StatusForbidden, "", "denied", "not yours"},
		{"a coded refusal", rpc.Refuse(http.StatusUnprocessableEntity, "test.signatureInvalid", errors.New("the signature does not verify")), http.StatusUnprocessableEntity, "test.signatureInvalid", "", "the signature does not verify"},
		{"a coded conflict", rpc.Refuse(http.StatusConflict, "test.staleRevision", fmt.Errorf("stale revision: %w", rpc.ErrConflict)), http.StatusConflict, "test.staleRevision", "conflict", "stale revision"},
		{"an ambiguous outcome", rpc.ErrAmbiguousEffect, http.StatusInternalServerError, "", "ambiguous", http.StatusText(http.StatusInternalServerError)},
		{"a handler failure", errors.New("internal detail"), http.StatusInternalServerError, "", "", http.StatusText(http.StatusInternalServerError)},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := serveStreamOp(t, streamHandler(rpc.ScopeNone, 1, c.fail), `{"names":["a","b"]}`)
			if rec.Code != http.StatusOK {
				t.Fatalf("answer %d, want 200 with an error event: %s", rec.Code, rec.Body.String())
			}
			data := errorEvent(t, rec.Body.String())
			if status, _ := data["status"].(float64); int(status) != c.wantStatus {
				t.Errorf("status %v, want %d (%v)", data["status"], c.wantStatus, data)
			}
			code, hasCode := data["code"]
			if c.wantCode == "" && hasCode {
				t.Errorf("code %v, want none (%v)", code, data)
			}
			if c.wantCode != "" && code != c.wantCode {
				t.Errorf("code %v, want %q (%v)", code, c.wantCode, data)
			}
			kind, hasKind := data["kind"].(string)
			if kind != c.wantKind || (c.wantKind == "" && hasKind) {
				t.Errorf("kind %v, want %q (%v)", data["kind"], c.wantKind, data)
			}
			if msg, _ := data["error"].(string); !strings.Contains(msg, c.wantText) || strings.Contains(msg, "internal detail") {
				t.Errorf("error %q, want it to carry %q and no 5xx cause", msg, c.wantText)
			}
		})
	}
}
