/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpchttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bbmumford/loom/pkg/rpc"
)

// A handler's coded refusal answers its own 4xx status, never retried, with
// its code beside the error text; a status outside 4xx or an empty code
// leaves the cause as it was, which answers 500.
func TestACodedRefusalAnswersItsStatusAndCode(t *testing.T) {
	cause := errors.New("approval: the vote's signature does not verify")
	refused := fmt.Errorf("op: %w", rpc.Refuse(http.StatusBadRequest, "approval_signature_invalid", cause))
	if got := classifyHandlerStatus(refused); got != http.StatusBadRequest {
		t.Fatalf("classifyHandlerStatus = %d", got)
	}
	if status, retryAfter, retry := classifyMeshError(refused); status != http.StatusBadRequest || retryAfter != "" || retry {
		t.Fatalf("classifyMeshError = %d, %q, retry %v", status, retryAfter, retry)
	}
	rec := httptest.NewRecorder()
	writeBridgeError(rec, handlerError(refused, "orbtr.approval.Grant"))
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusBadRequest || body["code"] != "approval_signature_invalid" || body["error"] == "" {
		t.Fatalf("answered %d %v", rec.Code, body)
	}
	if !errors.Is(refused, cause) {
		t.Fatal("the refusal does not unwrap to its cause")
	}
	for _, bad := range []error{
		rpc.Refuse(http.StatusInternalServerError, "approval_signature_invalid", cause),
		rpc.Refuse(http.StatusBadRequest, "", cause),
	} {
		if got := classifyHandlerStatus(bad); got != http.StatusInternalServerError {
			t.Errorf("a refusal outside 4xx or with no code answered %d", got)
		}
	}
}
