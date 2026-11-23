/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
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
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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

// cursorMessage is a message {int64 cursor = 1; string name = 2}.
func cursorMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("cursor_test.proto"),
		Package: proto.String("rpchttptest"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Page"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("cursor"), JsonName: proto.String("cursor"), Number: proto.Int32(1),
					Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("name"), JsonName: proto.String("name"), Number: proto.Int32(2),
					Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return dynamicpb.NewMessage(fd.Messages().Get(0))
}

// A frame's cursor field is its event id, and a reconnect's Last-Event-ID fills a
// request cursor the request left unset, never one it set; a message with no
// cursor field has no id and takes none.
func TestAStreamsCursorIsItsEventIDAndLastEventIDResumesIt(t *testing.T) {
	frame := cursorMessage(t)
	if _, ok := cursorOf(frame); ok {
		t.Error("an unset cursor answered an id")
	}
	frame.Set(frame.Descriptor().Fields().ByName("cursor"), protoreflect.ValueOfInt64(42))
	if id, ok := cursorOf(frame); !ok || id != "42" {
		t.Errorf("cursor id = %q %v, want 42", id, ok)
	}
	if _, ok := cursorOf(&structpb.Struct{}); ok {
		t.Error("a message with no cursor field answered an id")
	}

	r := httptest.NewRequest(http.MethodPost, "/rpc/x", nil)
	r.Header.Set("Last-Event-ID", "17")
	req := cursorMessage(t)
	resumeFromLastEventID(r, req)
	if got := req.Get(req.Descriptor().Fields().ByName("cursor")).Int(); got != 17 {
		t.Errorf("resumed cursor = %d, want 17", got)
	}
	set := cursorMessage(t)
	set.Set(set.Descriptor().Fields().ByName("cursor"), protoreflect.ValueOfInt64(5))
	resumeFromLastEventID(r, set)
	if got := set.Get(set.Descriptor().Fields().ByName("cursor")).Int(); got != 5 {
		t.Errorf("a set cursor was overwritten: %d", got)
	}
}

// An open stream that stays silent writes keepalive comments until the
// operation returns, and nothing after.
func TestAnOpenSilentStreamKeepsAlive(t *testing.T) {
	saved := streamKeepalive
	streamKeepalive = 20 * time.Millisecond
	defer func() { streamKeepalive = saved }()
	h := stubHandler(rpc.ScopeNone)
	h.Operation = "Quiet"
	h.Stream = func(ctx context.Context, s handlers.MessageStream) error {
		if _, err := s.Recv(); err != nil {
			return err
		}
		frame, _ := proto.Marshal(&structpb.Struct{})
		if err := s.Send(frame); err != nil {
			return err
		}
		time.Sleep(150 * time.Millisecond)
		return nil
	}
	rec := serveStreamOp(t, h, `{}`)
	body := rec.Body.String()
	if !strings.Contains(body, ": keepalive\n\n") {
		t.Errorf("no keepalive in %q", body)
	}
}
