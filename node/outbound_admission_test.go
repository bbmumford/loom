/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"

	"github.com/bbmumford/loom/pkg/dispatch"
)

// bidiAdmissionEnv marks the child process TestTheBidiSendEntriesAskTheOutboundAdmission
// runs its checks in; its presence is what makes the process the child.
const bidiAdmissionEnv = "LOOM_NODE_BIDI_OUTBOUND_ADMISSION"

// bidiAdmissionHeld is the line the child prints once every check has held, so a child
// that ran no test (a -test.run matching nothing exits 0) is not read as a pass.
const bidiAdmissionHeld = "outbound admission: CallViaBidi and BidiRPC.Call send nothing the admission refuses"

// bidiGatedRole is the role the child's admission refuses; every other role passes it.
const bidiGatedRole = "test.bidi.gated"

var errBidiGated = errors.New("gated: the child's admission refuses this role")

// TestTheBidiSendEntriesAskTheOutboundAdmission holds the two entries of this package
// that write a request this process originates onto a peer's bidi channel without an
// HWPCaller entry before them, ConnectionManager.CallViaBidi and BidiRPC.Call (the
// channel GetBidiRPC hands any holder of the manager), to the process's outbound
// admission: a request the installed admission refuses comes back with its refusal and
// no frame is written, a request it admits is written once, and call, through which
// the forwarder relays other nodes' requests, is not judged by it.
// dispatch.InstallAdmission refuses a second install and nothing outside dispatch
// empties it, so the install runs in a child process and this package's other tests
// keep a process with no admission.
func TestTheBidiSendEntriesAskTheOutboundAdmission(t *testing.T) {
	if _, child := os.LookupEnv(bidiAdmissionEnv); child {
		checkBidiSendEntries(t)
		fmt.Println(bidiAdmissionHeld)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTheBidiSendEntriesAskTheOutboundAdmission$", "-test.v")
	cmd.Env = append(os.Environ(), bidiAdmissionEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), bidiAdmissionHeld) {
		t.Fatalf("the child's checks did not hold (%v):\n%s", err, out)
	}
}

// checkBidiSendEntries installs the gating admission and sends through each entry, over
// one live bidi whose stream answers every request it is handed with a success.
func checkBidiSendEntries(t *testing.T) {
	var asked []string
	if err := dispatch.InstallAdmission(func(_ context.Context, role, method string, payload []byte) error {
		asked = append(asked, role+" "+method+" "+string(payload))
		if role == bidiGatedRole {
			return errBidiGated
		}
		return nil
	}); err != nil {
		t.Fatalf("InstallAdmission: %v", err)
	}
	m, sent := bidiManagerOn(t, wsSession())
	bidi, ok := m.GetBidiRPC(testNodeIDB)
	if !ok {
		t.Fatal("premise wrong: GetBidiRPC finds no bidi for the peer")
	}
	request := func(role string) *pb.RPCRequest {
		return &pb.RPCRequest{Id: "req-" + role, Handler: "test.bidi.Send", Payload: []byte("bytes"),
			Context: map[string]string{"role": role}}
	}
	refused := func(entry string, resp *pb.RPCResponse, err error) {
		t.Helper()
		if resp != nil || !errors.Is(err, dispatch.ErrNotAdmitted) || !errors.Is(err, errBidiGated) {
			t.Fatalf("%s returned (%v, %v), want the admission's refusal wrapped in dispatch.ErrNotAdmitted", entry, resp, err)
		}
		if n := sent(); n != 0 {
			t.Fatalf("%s wrote %d frame(s) of a request the admission refused", entry, n)
		}
	}

	resp, handled, err := m.CallViaBidi(context.Background(), testNodeIDB, request(bidiGatedRole))
	if !handled {
		t.Fatalf("CallViaBidi reported no bidi (%v); a refusal reported that way sends HWPCaller to its "+
			"dynamic-stream arm with the same request", err)
	}
	refused("CallViaBidi", resp, err)
	resp, err = bidi.Call(context.Background(), request(bidiGatedRole))
	refused("BidiRPC.Call", resp, err)
	want := bidiGatedRole + " test.bidi.Send bytes"
	if len(asked) != 2 || asked[0] != want || asked[1] != want {
		t.Fatalf("the admission was asked %q, want %q once per entry", asked, want)
	}

	if resp, handled, err := m.CallViaBidi(context.Background(), testNodeIDB, request("test.bidi.admitted")); !handled || err != nil || resp == nil || !resp.Success {
		t.Fatalf("CallViaBidi of an admitted request returned (%v, %v, %v)", resp, handled, err)
	}
	if n := sent(); n != 1 {
		t.Fatalf("CallViaBidi wrote %d frame(s) of an admitted request, want 1", n)
	}
	if resp, err := bidi.Call(context.Background(), request("test.bidi.admitted")); err != nil || resp == nil || !resp.Success {
		t.Fatalf("BidiRPC.Call of an admitted request returned (%v, %v)", resp, err)
	}
	if n := sent(); n != 2 {
		t.Fatalf("BidiRPC.Call wrote %d frame(s) in all after an admitted request, want 2", n)
	}

	if resp, err := bidi.call(context.Background(), request(bidiGatedRole)); err != nil || resp == nil || !resp.Success {
		t.Fatalf("call, the forwarder's relay, returned (%v, %v) for a request of another node", resp, err)
	}
	if n := sent(); n != 3 {
		t.Fatalf("call wrote %d frame(s) in all after a relayed request, want 3", n)
	}
	if len(asked) != 4 {
		t.Fatalf("the admission was asked %d times, want 4: the relay is not this process's call", len(asked))
	}
}
