/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// A handler's failure after acceptance reaches the opener carrying the four
// fields the HTTP stream's error event writes for the same error (error, code,
// status, kind: rpc.StreamFailureOf on the far side), so a mesh stream and an
// HTTP stream report it alike; and the opener's error, handed to the bridge's
// classification, answers the far side's status and fields unchanged.
func TestAHandlerStreamsFailureCarriesTheHTTPStreamsFields(t *testing.T) {
	for _, c := range []struct {
		name string
		ends error
		want dispatch.StreamFailure
	}{
		{"a denial", fmt.Errorf("the port closed by policy: %w", rpcscope.ErrDenied),
			dispatch.StreamFailure{Status: http.StatusForbidden, Kind: "denied"}},
		{"a coded refusal", rpc.Refuse(http.StatusConflict, "portClosed", errors.New("the port closed")),
			dispatch.StreamFailure{Error: "portClosed: the port closed", Code: "portClosed", Status: http.StatusConflict}},
		{"a handler's failure", errors.New("the cage went away"),
			dispatch.StreamFailure{Error: http.StatusText(http.StatusInternalServerError), Status: http.StatusInternalServerError}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			server := endingServer(t, &endingProbe{ends: c.ends})
			stream, err := dispatch.OpenHandlerStream(ctx, oneStreamSession{server: server, partition: "orbtr"}, typedPrincipalRequest())
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer stream.Close()
			if _, err := stream.Recv(ctx); err != nil {
				t.Fatalf("first frame: %v", err)
			}
			_, err = stream.Recv(ctx)
			got, ok := dispatch.StreamFailureOf(err)
			if !ok {
				t.Fatalf("the opener read %v, carrying no stream failure", err)
			}
			far := rpc.StreamFailureOf(c.ends)
			if got != far {
				t.Fatalf("the opener read %+v, the far side reported %+v", got, far)
			}
			if got.Status != c.want.Status || got.Kind != c.want.Kind || got.Code != c.want.Code ||
				(c.want.Error != "" && got.Error != c.want.Error) {
				t.Fatalf("the opener read %+v, want %+v", got, c.want)
			}
			if bridged := rpc.StreamFailureOf(err); bridged != far || rpc.HandlerStatus(err) != far.Status {
				t.Fatalf("the bridge reports the opener's error as %+v, status %d; the far side reported %+v",
					bridged, rpc.HandlerStatus(err), far)
			}
		})
	}
}
