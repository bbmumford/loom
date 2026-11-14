/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
)

// streamHandler is a server stream: it reads the request frame, then sends one
// frame per name in the request's "names" list, each {"name": name}, and fails
// with failAfter's error once that many frames are sent (never when negative).
func streamHandler(sc rpc.ScopeTag, failAfter int, fail error) rpc.Handler {
	h := stubHandler(sc)
	h.Operation = "Stream"
	h.Func = func(context.Context, proto.Message) (proto.Message, error) {
		return nil, errors.New("a stream; call it with a stream")
	}
	h.Stream = func(ctx context.Context, s handlers.MessageStream) error {
		raw, err := s.Recv()
		if err != nil {
			return err
		}
		req := &structpb.Struct{}
		if err := proto.Unmarshal(raw, req); err != nil {
			return err
		}
		if _, err := s.Recv(); err == nil {
			return errors.New("a second request frame on a server stream")
		}
		for i, v := range req.GetFields()["names"].GetListValue().GetValues() {
			if i == failAfter {
				return fail
			}
			frame, _ := structpb.NewStruct(map[string]any{"name": v.GetStringValue()})
			out, _ := proto.Marshal(frame)
			if err := s.Send(out); err != nil {
				return err
			}
		}
		if failAfter == 0 {
			return fail
		}
		return nil
	}
	return h
}

func serveStreamOp(t *testing.T, h rpc.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	reg := rpc.NewRegistry()
	reg.SetPresencePolicy(refPolicy("orbtr"))
	if err := reg.Register(h); err != nil {
		t.Fatalf("register: %v", err)
	}
	router := chi.NewRouter()
	RegisterAll(reg, router)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/rpc/"+h.FQN(), bytes.NewReader([]byte(body))))
	return rec
}

// A streamed operation's POST /rpc/<fqn> hands the protojson body to the stream
// as its one request frame and answers one server-sent event per frame, each the
// frame's protojson, in order.
func TestAStreamedOperationAnswersOneEventPerFrame(t *testing.T) {
	rec := serveStreamOp(t, streamHandler(rpc.ScopeNone, -1, nil), `{"names":["a","b","c"]}`)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("answer %d %q, want 200 text/event-stream: %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	want := "data: {\"name\":\"a\"}\n\ndata: {\"name\":\"b\"}\n\ndata: {\"name\":\"c\"}\n\n"
	if got := strings.ReplaceAll(rec.Body.String(), " ", ""); got != strings.ReplaceAll(want, " ", "") {
		t.Errorf("stream = %q, want %q", rec.Body.String(), want)
	}
}

// A refusal before the first frame is the bridge's error answer with its status:
// the scope judge's 403 before the stream runs, and the stream's own early
// error; a failure after a frame is one error event, the frames before it kept.
func TestAStreamRefusesBeforeItsFirstFrameAndFailsAfterIt(t *testing.T) {
	if rec := serveStreamOp(t, streamHandler(rpc.ScopeSubject, -1, nil), `{"names":["a"]}`); rec.Code != http.StatusForbidden {
		t.Errorf("an unproved caller of a subject stream: %d, want 403", rec.Code)
	}
	if rec := serveStreamOp(t, streamHandler(rpc.ScopeNone, 0, rpc.ErrConflict), `{"names":["a"]}`); rec.Code != http.StatusConflict ||
		strings.Contains(rec.Body.String(), "data:") {
		t.Errorf("an early refusal: %d %q, want 409 with no event", rec.Code, rec.Body.String())
	}
	rec := serveStreamOp(t, streamHandler(rpc.ScopeNone, 1, rpc.ErrConflict), `{"names":["a","b"]}`)
	body := strings.ReplaceAll(rec.Body.String(), " ", "")
	if rec.Code != http.StatusOK || !strings.Contains(body, `data:{"name":"a"}`) || !strings.Contains(body, "event:error\ndata:{\"error\":") ||
		strings.Contains(body, `"b"`) {
		t.Errorf("a late failure: %d %q", rec.Code, body)
	}
	if rec := serveStreamOp(t, streamHandler(rpc.ScopeNone, 1, errors.New("internal detail")), `{"names":["a","b"]}`); strings.Contains(rec.Body.String(), "internal detail") {
		t.Errorf("a 5xx cause leaked into the error event: %q", rec.Body.String())
	}
	if rec := serveStreamOp(t, streamHandler(rpc.ScopeNone, -1, nil), `{"nope`); rec.Code != http.StatusBadRequest {
		t.Errorf("a body that is not the request: %d, want 400", rec.Code)
	}
}
