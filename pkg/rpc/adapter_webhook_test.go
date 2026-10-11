/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/bbmumford/loom/node/handlers"
)

// webhookEventDescriptor describes a request with the string `payload` and
// `signature` fields extractWebhookFields reads. No generated type in this
// module has both as strings, so the tests build the descriptor here.
var webhookEventDescriptor = func() protoreflect.MessageDescriptor {
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("pkg/rpc/adapter_webhook_test.proto"),
		Package: proto.String("rpctest.webhook"),
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

// webhookEvent is a proto.Message over webhookEventDescriptor. NewInstance
// builds a request with reflect.New, so the zero value must be usable: the
// dynamic message is created on first ProtoReflect.
type webhookEvent struct{ m *dynamicpb.Message }

func (e *webhookEvent) ProtoReflect() protoreflect.Message {
	if e.m == nil {
		e.m = dynamicpb.NewMessage(webhookEventDescriptor)
	}
	return e.m
}

func newWebhookEvent(payload, signature string) *webhookEvent {
	e := &webhookEvent{}
	m := e.ProtoReflect()
	fields := webhookEventDescriptor.Fields()
	if payload != "" {
		m.Set(fields.ByName("payload"), protoreflect.ValueOfString(payload))
	}
	if signature != "" {
		m.Set(fields.ByName("signature"), protoreflect.ValueOfString(signature))
	}
	return e
}

const (
	webhookTestSecret  = "whsec_mesh_test"
	webhookTestPayload = `{"id":"evt_mesh","type":"invoice.paid"}`
)

// exportedWebhookTarget registers one handler in an rpc.Registry, exports it
// into a node/handlers.HandlerRegistry as the composition root does, and
// returns that registry with a counter of handler invocations. cfg nil
// registers a handler that declares no WebhookSignature.
func exportedWebhookTarget(t *testing.T, cfg *WebhookSignatureConfig) (*handlers.HandlerRegistry, string, *int) {
	t.Helper()
	calls := new(int)
	h := Handler{
		Namespace: "test",
		Domain:    "webhook",
		Operation: "Receive",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			*calls++
			return req, nil
		},
		Request:          (*webhookEvent)(nil),
		Response:         (*webhookEvent)(nil),
		Scope:            ScopeNone,
		WebhookSignature: cfg,
	}
	source := NewRegistry()
	if err := source.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	target := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(target); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	return target, h.FQN(), calls
}

// meshDispatch sends ev to fqn the way a proxied call arrives: the gateway
// marshals the typed request (rpchttp.Bridge.executeProxy) and the mesh
// request carries those bytes as its payload into HandlerRegistry.Dispatch.
func meshDispatch(t *testing.T, target *handlers.HandlerRegistry, fqn string, ev *webhookEvent) (*handlers.RPCResponse, error) {
	t.Helper()
	payload, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return target.Dispatch(context.Background(), &handlers.RPCRequest{
		ID:      "req-1",
		Handler: fqn,
		Payload: payload,
	})
}

func stripeConfig(secret func() string) *WebhookSignatureConfig {
	return &WebhookSignatureConfig{Algorithm: "stripe-hmac-sha256", SecretProvider: secret}
}

// TestExportedWebhookHandlerRefusesABadSignature: a webhook-class handler
// reached through the exported shim refuses, before its Func runs, a request
// whose signature does not verify against the configured secret.
func TestExportedWebhookHandlerRefusesABadSignature(t *testing.T) {
	now := time.Now().Unix()
	cases := []struct {
		name string
		ev   *webhookEvent
	}{
		{"signed with another secret", newWebhookEvent(webhookTestPayload, stripeSign(webhookTestPayload, "whsec_other", now))},
		{"payload altered after signing", newWebhookEvent(`{"id":"evt_forged"}`, stripeSign(webhookTestPayload, webhookTestSecret, now))},
		{"signature outside the skew window", newWebhookEvent(webhookTestPayload, stripeSign(webhookTestPayload, webhookTestSecret, now-3600))},
		{"no signature", newWebhookEvent(webhookTestPayload, "")},
		// An event with neither field set marshals to no bytes, so the shim
		// has nothing to decode and the check reads an empty request.
		{"an empty event, which reaches the shim as no payload bytes", newWebhookEvent("", "")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, fqn, calls := exportedWebhookTarget(t, stripeConfig(func() string { return webhookTestSecret }))
			resp, err := meshDispatch(t, target, fqn, c.ev)
			if !errors.Is(err, ErrWebhookSignatureInvalid) {
				t.Fatalf("Dispatch error = %v (resp %+v), want %v", err, resp, ErrWebhookSignatureInvalid)
			}
			if resp != nil && resp.Success {
				t.Fatalf("Dispatch reported success for a bad signature")
			}
			if *calls != 0 {
				t.Fatalf("handler Func ran %d times for a bad signature, want 0", *calls)
			}
		})
	}
}

// TestExportedWebhookHandlerRefusesAMissingSecret: with no signing secret
// configured at the destination, a correctly signed request is still refused
// before the handler's Func runs, for both secret sources.
func TestExportedWebhookHandlerRefusesAMissingSecret(t *testing.T) {
	const envVar = "LOOM_RPC_TEST_WEBHOOK_SECRET"
	ev := newWebhookEvent(webhookTestPayload, stripeSign(webhookTestPayload, webhookTestSecret, time.Now().Unix()))
	cases := []struct {
		name string
		cfg  *WebhookSignatureConfig
	}{
		{"empty env var", &WebhookSignatureConfig{Algorithm: "stripe-hmac-sha256", SecretEnvVar: envVar}},
		{"provider returns empty", stripeConfig(func() string { return "" })},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(envVar, "")
			target, fqn, calls := exportedWebhookTarget(t, c.cfg)
			resp, err := meshDispatch(t, target, fqn, ev)
			if !errors.Is(err, ErrWebhookSecretUnset) {
				t.Fatalf("Dispatch error = %v (resp %+v), want %v", err, resp, ErrWebhookSecretUnset)
			}
			if resp != nil && resp.Success {
				t.Fatalf("Dispatch reported success with no secret configured")
			}
			if *calls != 0 {
				t.Fatalf("handler Func ran %d times with no secret configured, want 0", *calls)
			}
		})
	}
}

// TestExportedWebhookHandlerAdmitsAValidSignature: a request signed with the
// configured secret passes the check and reaches the handler, and the
// response carries the decoded request back.
func TestExportedWebhookHandlerAdmitsAValidSignature(t *testing.T) {
	const envVar = "LOOM_RPC_TEST_WEBHOOK_SECRET"
	t.Setenv(envVar, webhookTestSecret)
	target, fqn, calls := exportedWebhookTarget(t, &WebhookSignatureConfig{Algorithm: "stripe-hmac-sha256", SecretEnvVar: envVar})
	ev := newWebhookEvent(webhookTestPayload, stripeSign(webhookTestPayload, webhookTestSecret, time.Now().Unix()))

	resp, err := meshDispatch(t, target, fqn, ev)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Fatalf("Dispatch = %+v, want success", resp)
	}
	if *calls != 1 {
		t.Fatalf("handler Func ran %d times, want 1", *calls)
	}
	got := &webhookEvent{}
	if err := proto.Unmarshal(resp.Payload, got); err != nil {
		t.Fatalf("proto.Unmarshal response: %v", err)
	}
	if p := got.ProtoReflect().Get(webhookEventDescriptor.Fields().ByName("payload")).String(); p != webhookTestPayload {
		t.Fatalf("response payload = %q, want %q", p, webhookTestPayload)
	}
}

// clearFuncWhenAsked registers one webhook-class handler whose SecretProvider
// clears Func on the registry's stored handler and then returns the signing
// secret. SecretProvider is the caller code verifyWebhookSignature runs after
// the Func refusal and before the Func call, and Get hands back the pointer the
// shim and Dispatch both hold, so it reaches that window without a second
// goroutine. Returns the registry, the stored handler, the FQN, a signed
// request and a count of Func invocations.
func clearFuncWhenAsked(t *testing.T) (*Registry, *Handler, string, *webhookEvent, *int) {
	t.Helper()
	calls := new(int)
	reg := NewRegistry()
	h := Handler{
		Namespace: "test",
		Domain:    "window",
		Operation: "Receive",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			*calls++
			return req, nil
		},
		Request:  (*webhookEvent)(nil),
		Response: (*webhookEvent)(nil),
		Scope:    ScopeNone,
	}
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stored, ok := reg.Get(h.FQN())
	if !ok {
		t.Fatalf("Get %q after Register: not found", h.FQN())
	}
	stored.WebhookSignature = stripeConfig(func() string {
		stored.Func = nil
		return webhookTestSecret
	})
	ev := newWebhookEvent(webhookTestPayload, stripeSign(webhookTestPayload, webhookTestSecret, time.Now().Unix()))
	return reg, stored, h.FQN(), ev, calls
}

// TestShimCallsTheFuncItAdmitted: registryShim.ExecuteRPC admits the request on
// the Func it read before the payload and calls that Func, so clearing Func on
// the stored handler while the call is inside verifyWebhookSignature neither
// dereferences nil nor turns the admitted call into a refusal.
func TestShimCallsTheFuncItAdmitted(t *testing.T) {
	_, stored, _, ev, calls := clearFuncWhenAsked(t)
	payload, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	shim := newRegistryShim(stored)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("the shim called a Func cleared after it admitted the request: %v", r)
		}
	}()
	resp, err := shim.ExecuteRPC(context.Background(), &handlers.RPCRequest{ID: "req-1", Payload: payload})
	if err != nil {
		t.Fatalf("ExecuteRPC: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Fatalf("ExecuteRPC = %+v, want success", resp)
	}
	if *calls != 1 {
		t.Fatalf("admitted handler Func ran %d times, want 1", *calls)
	}
}

// TestDispatchCallsTheFuncItAdmitted: Registry.Dispatch admits the request on
// the Func it read before the scope check and calls that Func, so clearing Func
// on the stored handler while the call is inside verifyWebhookSignature neither
// dereferences nil nor turns the admitted call into a refusal.
func TestDispatchCallsTheFuncItAdmitted(t *testing.T) {
	reg, _, fqn, ev, calls := clearFuncWhenAsked(t)
	payload, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Dispatch called a Func cleared after it admitted the request: %v", r)
		}
	}()
	out, err := reg.Dispatch(context.Background(), fqn, payload)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("Dispatch returned an empty response for an admitted call")
	}
	if *calls != 1 {
		t.Fatalf("admitted handler Func ran %d times, want 1", *calls)
	}
}

// TestExportedHandlerWithoutWebhookSignatureIsUnchecked: a handler that
// declares no WebhookSignature runs on a request carrying no signature, so
// the check applies only to handlers that declare it.
func TestExportedHandlerWithoutWebhookSignatureIsUnchecked(t *testing.T) {
	target, fqn, calls := exportedWebhookTarget(t, nil)

	resp, err := meshDispatch(t, target, fqn, newWebhookEvent(webhookTestPayload, ""))
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if resp == nil || !resp.Success {
		t.Fatalf("Dispatch = %+v, want success", resp)
	}
	if *calls != 1 {
		t.Fatalf("handler Func ran %d times, want 1", *calls)
	}
}
