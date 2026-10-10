/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/grade"
	"google.golang.org/protobuf/proto"
)

// These tests cover a route refusal made by a later hop: the refusing node
// stamps it on its failed response (StampRefusal) and HWPCaller rebuilds it at
// each of the five places it reads a response, so the caller gets the typed
// refusal and not a handler rejection.

// wireFinder answers every request with one fixed response, over the bidi
// channel when viaBidi is set and over a dynamic stream of its session
// otherwise. noRoutes sends Call down its callOnce path.
type wireFinder struct {
	mockFinder
	noRoutes bool
	viaBidi  bool
	resp     *pb.RPCResponse
}

func newWireFinder(resp *pb.RPCResponse, noRoutes, viaBidi bool) *wireFinder {
	return &wireFinder{
		mockFinder: mockFinder{session: &rpcMockSession{resp: resp}},
		noRoutes:   noRoutes,
		viaBidi:    viaBidi,
		resp:       resp,
	}
}

func (f *wireFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	if f.noRoutes {
		return nil
	}
	return f.mockFinder.FindRoutes(ctx, role, handler)
}

func (f *wireFinder) CallViaBidi(context.Context, string, *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	if !f.viaBidi {
		return nil, false, nil
	}
	return proto.Clone(f.resp).(*pb.RPCResponse), true, nil
}

// responsePaths are the five places HWPCaller reads a response.
var responsePaths = []struct {
	name string
	call func(resp *pb.RPCResponse) ([]byte, error)
}{
	{"a probe over a dynamic stream", func(resp *pb.RPCResponse) ([]byte, error) {
		return callWith(newWireFinder(resp, false, false))
	}},
	{"a probe over bidi", func(resp *pb.RPCResponse) ([]byte, error) {
		return callWith(newWireFinder(resp, false, true))
	}},
	{"callOnce over a dynamic stream", func(resp *pb.RPCResponse) ([]byte, error) {
		return callWith(newWireFinder(resp, true, false))
	}},
	{"callOnce over bidi", func(resp *pb.RPCResponse) ([]byte, error) {
		return callWith(newWireFinder(resp, true, true))
	}},
	{"CallNode", func(resp *pb.RPCResponse) ([]byte, error) {
		caller := NewHWPCaller(newWireFinder(resp, false, true))
		defer caller.Close()
		return caller.CallNode(context.Background(), "node-1", "svc", "svc.Op", nil)
	}},
}

func callWith(f *wireFinder) ([]byte, error) {
	defer forgetSession(f.session)
	caller := NewHWPCaller(f)
	defer caller.Close()
	return caller.Call(context.Background(), "svc", "svc.Op", nil)
}

// A refusal stamped on a failed response comes back from every response path
// as the same typed refusal, with its fields, under the refusing node's own
// text, and never as a handler error.
func TestARefusalStampedOnAResponseReachesTheCallerTyped(t *testing.T) {
	for _, refusal := range []struct {
		name string
		err  error
	}{
		{"a grade refusal", &GradeTooLowError{Handler: "svc.Op", Class: "critical", Current: grade.GradeC, Required: grade.GradeB}},
		{"an undeclared class", &GradeTooLowError{Handler: "svc.Op", Class: "turbo", UnknownClass: true}},
		{"a deadline refusal", &RouteDeadlineError{Handler: "svc.Op", Remaining: 800 * time.Millisecond, Need: 5 * time.Second}},
		{"an expired deadline", &RouteDeadlineError{Handler: "svc.Op", Remaining: -40 * time.Millisecond}},
	} {
		for _, path := range responsePaths {
			t.Run(refusal.name+"/"+path.name, func(t *testing.T) {
				resp := &pb.RPCResponse{
					Id:      "req-1",
					Success: false,
					Error:   "forward svc.Op failed (hop 2): " + refusal.err.Error(),
				}
				// The server stamps the error Forward returned, which may be
				// wrapped; the refusal must be found through the wrapping.
				StampRefusal(resp, fmt.Errorf("re-route: %w", refusal.err))

				out, err := path.call(resp)
				if err == nil || out != nil {
					t.Fatalf("returned (%q, %v) for a refused response", out, err)
				}
				if errors.Is(err, ErrHandlerError) {
					t.Fatalf("returned %v, a handler error; the refusal was a route's, not the "+
						"handler's", err)
				}
				switch want := refusal.err.(type) {
				case *GradeTooLowError:
					var got *GradeTooLowError
					if !errors.Is(err, ErrGradeTooLow) || !errors.As(err, &got) {
						t.Fatalf("returned %v; want a *GradeTooLowError", err)
					}
					if *got != *want {
						t.Errorf("rebuilt refusal %+v, the refusing node's was %+v", *got, *want)
					}
				case *RouteDeadlineError:
					var got *RouteDeadlineError
					if !errors.Is(err, ErrRouteDeadline) || !errors.As(err, &got) {
						t.Fatalf("returned %v; want a *RouteDeadlineError", err)
					}
					if *got != *want {
						t.Errorf("rebuilt refusal %+v, the refusing node's was %+v", *got, *want)
					}
				}
				if !strings.Contains(err.Error(), "hop 2") {
					t.Errorf("the error text %q lost the refusing node's own words", err)
				}
			})
		}
	}
}

// Only a refusal is stamped, and only a failed response is read as one: a
// failed response with no stamp stays a handler error on every path, an error
// that is no refusal stamps nothing, and a successful response is returned
// whatever its metadata says.
func TestOnlyAStampedFailureIsReadAsARefusal(t *testing.T) {
	plain := &pb.RPCResponse{Id: "req-1", Success: false, Error: "not found"}
	StampRefusal(plain, errors.New("open RPC stream: connection reset"))
	if plain.Metadata != nil {
		t.Fatalf("an error that is no refusal stamped %v on the response", plain.Metadata)
	}
	for _, path := range responsePaths {
		t.Run("unstamped/"+path.name, func(t *testing.T) {
			_, err := path.call(plain)
			if !errors.Is(err, ErrHandlerError) || isRouteRefusal(err) {
				t.Fatalf("returned %v; an unstamped failed response is the handler's rejection", err)
			}
		})
	}

	served := &pb.RPCResponse{Id: "req-1", Success: true, Payload: []byte("ran")}
	StampRefusal(served, &GradeTooLowError{Handler: "svc.Op", Current: grade.GradeC, Required: grade.GradeB})
	for _, path := range responsePaths {
		t.Run("successful/"+path.name, func(t *testing.T) {
			out, err := path.call(served)
			if err != nil || string(out) != "ran" {
				t.Fatalf("returned (%q, %v); a successful response is the handler's answer", out, err)
			}
		})
	}
}
