package slog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/bbmumford/loom/pkg/trace"
)

// A record carries the bound subsystem, the canonical decision/reason for a
// security deny, and the trace id auto-injected from the loom trace ctx key.
func TestSecurityDeny_VocabularyAndTraceInjection(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx := trace.WithID(context.Background(), "trace-xyz")
	New("security.middleware.auth").SecurityDeny(ctx, "denied", "org_access_denied", "user-1", errors.New("boom"))

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("record not JSON: %v (%q)", err, buf.String())
	}
	for k, want := range map[string]string{
		KeySubsystem: "security.middleware.auth",
		KeyReason:    "org_access_denied",
		KeyDecision:  DecisionDeny,
		KeyActor:     "user-1",
		KeyTrace:     "trace-xyz",
	} {
		if got, _ := rec[k].(string); got != want {
			t.Errorf("field %q = %q, want %q", k, got, want)
		}
	}
	if _, ok := rec[KeyErr]; !ok {
		t.Errorf("err field missing on an infra-driven deny")
	}
}

// With no trace set, the trace attribute is dropped (zero-cost majority path).
func TestNoTrace_DropsTraceAttr(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	New("mesh.gossip").Info(context.Background(), "promoted")

	var rec map[string]any
	_ = json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec)
	if _, present := rec[KeyTrace]; present {
		t.Errorf("trace attr present with no trace in ctx")
	}
	if got, _ := rec[KeySubsystem].(string); got != "mesh.gossip" {
		t.Errorf("subsystem = %q, want mesh.gossip", got)
	}
}
