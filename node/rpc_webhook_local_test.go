/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
)

// localWebhookDescriptor describes a request with the string `payload` and
// `signature` fields the webhook check reads off the decoded request. No
// generated type loom imports has both as strings, so the test builds one.
var localWebhookDescriptor = func() protoreflect.MessageDescriptor {
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("node/rpc_webhook_local_test.proto"),
		Package: proto.String("nodetest.webhook"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Event"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("payload"), JsonName: proto.String("payload"), Number: proto.Int32(1), Type: str, Label: opt},
				{Name: proto.String("signature"), JsonName: proto.String("signature"), Number: proto.Int32(2), Type: str, Label: opt},
			},
		}},
	}, nil)
	if err != nil {
		panic(err)
	}
	return fd.Messages().Get(0)
}()

// localWebhookEvent is a proto.Message over localWebhookDescriptor. The
// registry builds a request with reflect.New, so the zero value creates its
// dynamic message on first ProtoReflect.
type localWebhookEvent struct{ m *dynamicpb.Message }

func (e *localWebhookEvent) ProtoReflect() protoreflect.Message {
	if e.m == nil {
		e.m = dynamicpb.NewMessage(localWebhookDescriptor)
	}
	return e.m
}

func newLocalWebhookEvent(payload, signature string) *localWebhookEvent {
	e := &localWebhookEvent{}
	m := e.ProtoReflect()
	fields := localWebhookDescriptor.Fields()
	m.Set(fields.ByName("payload"), protoreflect.ValueOfString(payload))
	if signature != "" {
		m.Set(fields.ByName("signature"), protoreflect.ValueOfString(signature))
	}
	return e
}

// stripeSignedHeader builds the `t=<unix>,v1=<hex>` header
// rpc.VerifyStripeHMACSHA256 checks: HMAC-SHA256 over "<t>.<payload>".
func stripeSignedHeader(payload, secret string, at time.Time) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.%s", at.Unix(), payload)
	return fmt.Sprintf("t=%d,v1=%s", at.Unix(), hex.EncodeToString(mac.Sum(nil)))
}

// A webhook-class handler reached over the mesh is refused before its Func
// whenever the signature does not verify, on the path a mesh request takes
// into this node: RPCServer.handleRequest finds the FQN in the registry
// rpc.ExportToHandlerRegistry filled, and executeLocal dispatches it through
// the registry shim, which runs the check rpc.Registry.Dispatch runs. A request
// with no payload bytes is checked like any other: the shim has nothing to
// decode, and the check then reads an event with no payload and no signature.
// The admitted case runs the Func exactly once, so a server that refused every
// request would fail it.
func TestAMeshWebhookRequestIsVerifiedBeforeTheHandlerRuns(t *testing.T) {
	const signingSecret = "whsec_node_local"
	const body = `{"id":"evt_node","type":"invoice.paid"}`

	secret := signingSecret
	calls := 0
	h := rpc.Handler{
		Namespace: "test", Domain: "webhook", Operation: "Receive",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			calls++
			return req, nil
		},
		Request:  (*localWebhookEvent)(nil),
		Response: (*localWebhookEvent)(nil),
		Scope:    rpc.ScopeNone,
		WebhookSignature: &rpc.WebhookSignatureConfig{
			Algorithm:      "stripe-hmac-sha256",
			SecretProvider: func() string { return secret },
		},
	}
	source := rpc.NewRegistry()
	if err := source.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	exported := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(exported); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	server := NewRPCServer(exported)

	now := time.Now()
	for i, tc := range []struct {
		name      string
		secret    string
		signature string
		noPayload bool
		refusal   error
	}{
		{"an unsigned event", signingSecret, "", false, rpc.ErrWebhookSignatureInvalid},
		{"a signature under another secret", signingSecret, stripeSignedHeader(body, "whsec_other", now), false, rpc.ErrWebhookSignatureInvalid},
		{"a signature outside the skew", signingSecret, stripeSignedHeader(body, signingSecret, now.Add(-time.Hour)), false, rpc.ErrWebhookSignatureInvalid},
		{"no secret configured", "", stripeSignedHeader(body, signingSecret, now), false, rpc.ErrWebhookSecretUnset},
		{"a request with no payload bytes", signingSecret, "", true, rpc.ErrWebhookSignatureInvalid},
		{"a valid signature", signingSecret, stripeSignedHeader(body, signingSecret, now), false, nil},
	} {
		secret = tc.secret
		calls = 0
		var payload []byte
		if !tc.noPayload {
			encoded, err := proto.Marshal(newLocalWebhookEvent(body, tc.signature))
			if err != nil {
				t.Fatalf("%s: proto.Marshal: %v", tc.name, err)
			}
			payload = encoded
		}
		resp := server.handleRequest(context.Background(), &pb.RPCRequest{
			Id:      fmt.Sprintf("webhook-%d", i),
			Handler: h.FQN(),
			Payload: payload,
		})
		if resp == nil {
			t.Fatalf("%s: handleRequest returned nil", tc.name)
		}

		if tc.refusal != nil {
			if resp.Success || !strings.Contains(resp.Error, tc.refusal.Error()) {
				t.Errorf("%s: response success=%v error=%q, want a refusal carrying %q",
					tc.name, resp.Success, resp.Error, tc.refusal)
			}
			if calls != 0 {
				t.Errorf("%s: the handler ran %d times on a request that does not verify", tc.name, calls)
			}
			continue
		}
		if !resp.Success {
			t.Fatalf("%s: refused: %s", tc.name, resp.Error)
		}
		if calls != 1 {
			t.Fatalf("%s: the handler ran %d times, want 1", tc.name, calls)
		}
		echoed := &localWebhookEvent{}
		if err := proto.Unmarshal(resp.Payload, echoed); err != nil {
			t.Fatalf("%s: response payload: %v", tc.name, err)
		}
		if got := echoed.ProtoReflect().Get(localWebhookDescriptor.Fields().ByName("payload")).String(); got != body {
			t.Fatalf("%s: handler saw payload %q, want %q", tc.name, got, body)
		}
	}
}
