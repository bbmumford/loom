/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// jsonNamesAnswer is the response every surface below answers: a message whose
// populated fields are named differently in their .proto source (type_name,
// json_name, oneof_index, proto3_optional) and in their generated JSON names
// (typeName, jsonName, oneofIndex, proto3Optional), so a response keyed by the
// proto names is told apart from one keyed by the JSON names.
func jsonNamesAnswer() *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:           proto.String("field"),
		TypeName:       proto.String(".orbtr.test.Message"),
		JsonName:       proto.String("aliased"),
		OneofIndex:     proto.Int32(1),
		Proto3Optional: proto.Bool(true),
	}
}

// jsonNamesOp is the operation the surfaces serve, answering jsonNamesAnswer.
func jsonNamesOp() rpc.Handler {
	h := stubHandler(rpc.ScopeNone)
	h.Operation = "JSONNames"
	h.Request = (*descriptorpb.FieldDescriptorProto)(nil)
	h.Response = (*descriptorpb.FieldDescriptorProto)(nil)
	h.Func = func(context.Context, proto.Message) (proto.Message, error) {
		return jsonNamesAnswer(), nil
	}
	return h
}

// answeringCaller is the mesh caller a proxied operation reaches: it answers
// every call with the wire bytes of answer.
type answeringCaller struct{ answer proto.Message }

func (c *answeringCaller) Call(context.Context, string, string, []byte) ([]byte, error) {
	return proto.Marshal(c.answer)
}
func (*answeringCaller) Close() {}

// requireGeneratedJSONNames holds body, one JSON object, to the generated JSON
// name of each field msg populates: its keys are exactly those names, and at
// least one of them differs from its field's proto name, so the check tells
// the two namings apart.
func requireGeneratedJSONNames(t *testing.T, surface string, body []byte, msg proto.Message) {
	t.Helper()
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(body, &keys); err != nil {
		t.Fatalf("%s: the response %q is not one JSON object: %v", surface, body, err)
	}
	var got, want []string
	for k := range keys {
		got = append(got, k)
	}
	differs := false
	msg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		want = append(want, fd.JSONName())
		differs = differs || fd.JSONName() != string(fd.Name())
		return true
	})
	sort.Strings(got)
	sort.Strings(want)
	if !differs {
		t.Fatalf("%s: every populated field's JSON name equals its proto name; the check cannot tell them apart", surface)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("%s: the response names its fields %v; want their generated JSON names %v", surface, got, want)
	}
}

// A /rpc response names each field by the JSON name its generated descriptor
// gives it (FieldDescriptor.JSONName), on each surface that answers one: a Func
// the bridge runs in its own process (executeLocal), a call it proxies over the
// mesh (executeProxy), and each frame of a streamed operation (httpStream.Send).
// That is the default naming of protojson, the one every proto field and every
// client agree on, so a change to the marshal options that keys a response by
// any other name fails here.
func TestRPCResponsesNameEachFieldByItsGeneratedJSONName(t *testing.T) {
	t.Run("a Func run in this process", func(t *testing.T) {
		rec := serveStreamOp(t, jsonNamesOp(), "{}")
		if rec.Code != http.StatusOK {
			t.Fatalf("answer %d: %s", rec.Code, rec.Body.String())
		}
		requireGeneratedJSONNames(t, "executeLocal", rec.Body.Bytes(), jsonNamesAnswer())
	})

	t.Run("a call proxied over the mesh", func(t *testing.T) {
		previous := dispatch.GetCaller()
		dispatch.SetCaller(&answeringCaller{answer: jsonNamesAnswer()})
		t.Cleanup(func() { dispatch.SetCaller(previous) })
		h := jsonNamesOp()
		h.Func = nil
		rec := serveStreamOp(t, h, "{}")
		if rec.Code != http.StatusOK {
			t.Fatalf("answer %d: %s", rec.Code, rec.Body.String())
		}
		requireGeneratedJSONNames(t, "executeProxy", rec.Body.Bytes(), jsonNamesAnswer())
	})

	t.Run("a frame of a streamed operation", func(t *testing.T) {
		h := jsonNamesOp()
		h.Operation = "JSONNamesStream"
		h.Func = func(context.Context, proto.Message) (proto.Message, error) {
			return nil, errors.New("a stream; call it with a stream")
		}
		h.Stream = func(_ context.Context, s handlers.MessageStream) error {
			if _, err := s.Recv(); err != nil {
				return err
			}
			frame, err := proto.Marshal(jsonNamesAnswer())
			if err != nil {
				return err
			}
			return s.Send(frame)
		}
		rec := serveStreamOp(t, h, "{}")
		if rec.Code != http.StatusOK {
			t.Fatalf("answer %d: %s", rec.Code, rec.Body.String())
		}
		var frames []string
		for _, line := range strings.Split(rec.Body.String(), "\n") {
			if data, ok := strings.CutPrefix(line, "data:"); ok {
				frames = append(frames, strings.TrimSpace(data))
			}
		}
		if len(frames) != 1 {
			t.Fatalf("the stream answered %q; want one data event", rec.Body.String())
		}
		requireGeneratedJSONNames(t, "httpStream.Send", []byte(frames[0]), jsonNamesAnswer())
	})
}
