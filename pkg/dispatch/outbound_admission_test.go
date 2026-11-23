/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package dispatch

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// The outbound admission is held by behaviour: a call it refuses must leave
// nothing on any session, through every send entry this package exports, and the
// call it admits must leave. Each case counts what reached a stream or a finder
// path, because a refusal that also sent would satisfy an error check alone.

// errGated is the test admission's refusal of the gated role.
var errGated = errors.New("gated: the test admission refuses this role")

// gatedRole is the role the test admission refuses; every other role passes it.
const gatedRole = "test.gated"

// asked is one question the test admission was asked.
type asked struct {
	role, method string
	payload      []byte
}

// installGate installs an admission that refuses gatedRole and records every
// question, and restores the process's empty admission and caller when the test
// ends, because both are process-wide.
func installGate(t *testing.T) *[]asked {
	t.Helper()
	resetOutbound()
	t.Cleanup(resetOutbound)
	var got []asked
	if err := InstallAdmission(func(_ context.Context, role, method string, payload []byte) error {
		got = append(got, asked{role, method, append([]byte(nil), payload...)})
		if role == gatedRole {
			return errGated
		}
		return nil
	}); err != nil {
		t.Fatalf("InstallAdmission: %v", err)
	}
	return &got
}

// resetOutbound empties the process-wide admission and caller.
func resetOutbound() {
	globalMu.Lock()
	admission = nil
	globalCaller = nil
	globalMu.Unlock()
}

// admissionFinder reaches one graded session by every path and records which path
// a call took.
type admissionFinder struct {
	recordingFinder
	node *gradedSession
}

func (f *admissionFinder) MeshSessionForNode(string) (aether.Session, bool) { return f.node, true }

func newAdmissionFinder() *admissionFinder {
	s := newGradedSession(aether.ProtoWebSocket, "pong")
	return &admissionFinder{
		recordingFinder: recordingFinder{
			mockFinder: mockFinder{session: s},
			bidiResp:   &pb.RPCResponse{Success: true, Payload: []byte("pong")},
			bidiOK:     true,
		},
		node: s,
	}
}

// wantNothingSent fails unless err is the admission's refusal and no path of the
// finder was reached and no request was written on the session.
func wantNothingSent(t *testing.T, err error, f *admissionFinder) {
	t.Helper()
	if !errors.Is(err, ErrNotAdmitted) || !errors.Is(err, errGated) {
		t.Fatalf("err = %v, want the admission's refusal wrapped in ErrNotAdmitted", err)
	}
	if !Unsent(err) {
		t.Fatalf("Unsent(%v) = false: nothing was sent", err)
	}
	if f.findRoutes != 0 || f.findSessions != 0 || f.bidiCalls != 0 {
		t.Fatalf("routes %d, sessions %d, bidi %d: a refused call reached a path", f.findRoutes, f.findSessions, f.bidiCalls)
	}
	if n := f.node.sends.Load(); n != 0 {
		t.Fatalf("%d requests written on the session after the refusal", n)
	}
}

// Call refuses on both of its paths, the probed routes and callOnce, and the
// admission is asked with the call's role, its full method name and its bytes.
func TestCallSendsNothingTheAdmissionRefuses(t *testing.T) {
	for _, path := range callPaths {
		t.Run(path.name, func(t *testing.T) {
			got := installGate(t)
			s := newGradedSession(aether.ProtoWebSocket, "pong")
			c := NewHWPCaller(path.finder(s))
			defer c.Close()

			_, err := c.Call(context.Background(), gatedRole, gatedRole+".Op", []byte("body"))
			if !errors.Is(err, ErrNotAdmitted) || !errors.Is(err, errGated) {
				t.Fatalf("err = %v, want the admission's refusal", err)
			}
			if n := s.sends.Load(); n != 0 {
				t.Fatalf("%d requests sent after the refusal", n)
			}
			if len(*got) != 1 || (*got)[0].role != gatedRole || (*got)[0].method != gatedRole+".Op" || !bytes.Equal((*got)[0].payload, []byte("body")) {
				t.Fatalf("admission asked %+v, want once with the call's role, method and bytes", *got)
			}

			resp, err := c.Call(context.Background(), "test.open", "test.open.Op", []byte("body"))
			if err != nil || string(resp) != "pong" {
				t.Fatalf("admitted call = %q, %v; want pong", resp, err)
			}
			if s.sends.Load() == 0 {
				t.Fatal("the admitted call wrote nothing on the session")
			}
		})
	}
}

// A cached answer is not a way round the admission: CallCacheable and Call's own
// cache are both read only after it admits the call.
func TestTheResultCacheAnswersNothingTheAdmissionRefuses(t *testing.T) {
	installGate(t)
	f := newAdmissionFinder()
	c := NewHWPCaller(f)
	defer c.Close()
	fqn := gatedRole + "/" + gatedRole + ".Op"
	c.StoreCachedResult(fqn, []byte("body"), []byte("cached"), time.Minute)
	c.SetCacheableHandlers(map[string]time.Duration{fqn: time.Minute})

	out, err := c.CallCacheable(context.Background(), gatedRole, gatedRole+".Op", []byte("body"), time.Minute)
	if out != nil {
		t.Fatalf("CallCacheable answered %q from the cache for a refused call", out)
	}
	wantNothingSent(t, err, f)
	out, err = c.Call(context.Background(), gatedRole, gatedRole+".Op", []byte("body"))
	if out != nil {
		t.Fatalf("Call answered %q from the cache for a refused call", out)
	}
	wantNothingSent(t, err, f)
}

// CallNode builds and sends its own request through the finder's bidi channel and
// never passes through Call.
func TestCallNodeSendsNothingTheAdmissionRefuses(t *testing.T) {
	installGate(t)
	f := newAdmissionFinder()
	c := NewHWPCaller(f)
	defer c.Close()

	_, err := c.CallNode(context.Background(), "n1", gatedRole, gatedRole+".Op", []byte("body"))
	wantNothingSent(t, err, f)

	if _, err := c.CallNode(context.Background(), "n1", "test.open", "test.open.Op", []byte("body")); err != nil {
		t.Fatalf("admitted CallNode: %v", err)
	}
	if f.bidiCalls != 1 {
		t.Fatalf("bidi calls %d, want the admitted call's one", f.bidiCalls)
	}
}

// A handler stream is refused before it is opened, whether opened through the
// caller or straight onto a session with OpenHandlerStream.
func TestAStreamTheAdmissionRefusesIsNeverOpened(t *testing.T) {
	installGate(t)
	f := newAdmissionFinder()
	c := NewHWPCaller(f)
	defer c.Close()

	_, err := c.OpenNodeStream(context.Background(), "n1", gatedRole, gatedRole+".Stream")
	wantNothingSent(t, err, f)
	if n := atomic.LoadInt32(&f.node.openCount); n != 0 {
		t.Fatalf("%d streams opened for a refused stream", n)
	}

	req := buildRPCRequestCtx(context.Background(), gatedRole, gatedRole+".Stream", []byte("open"), 0)
	_, err = OpenHandlerStream(context.Background(), f.node, req)
	wantNothingSent(t, err, f)
	if n := atomic.LoadInt32(&f.node.openCount); n != 0 {
		t.Fatalf("%d streams opened on the session for a refused stream", n)
	}
}

// One admission per process: a second install and a nil function are refused,
// and the first stays in force.
func TestAnAdmissionIsInstalledOnce(t *testing.T) {
	installGate(t)
	if err := InstallAdmission(func(context.Context, string, string, []byte) error { return nil }); !errors.Is(err, ErrAdmissionInstalled) {
		t.Fatalf("second InstallAdmission = %v, want ErrAdmissionInstalled", err)
	}
	if err := InstallAdmission(nil); err == nil {
		t.Fatal("InstallAdmission(nil) accepted")
	}
	if err := Admit(context.Background(), gatedRole, gatedRole+".Op", nil); !errors.Is(err, errGated) {
		t.Fatalf("Admit after the refused installs = %v, want the first admission's refusal", err)
	}
}
