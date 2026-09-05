/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"testing"

	"github.com/bbmumford/loom/ports"
)

// stubHandler is the handler metadata surface the validator judges.
type stubHandler struct {
	name         string
	requiresAuth bool
	authTypes    []string
	scopes       []string
}

func (s stubHandler) Name() string                { return s.name }
func (s stubHandler) RequiresAuth() bool          { return s.requiresAuth }
func (s stubHandler) AllowedAuthTypes() []string  { return s.authTypes }
func (s stubHandler) Scopes() []string            { return s.scopes }
func (s stubHandler) Scope() ports.Scope          { return ports.Scope("") }
func (s stubHandler) AllowedPartitions() []string { return nil }

var _ ports.SecureHandler = stubHandler{}

func TestDefaultAuthValidatorAdmitsHandlersThatNeedNoAuth(t *testing.T) {
	err := DefaultAuthValidator().ValidateExecutionAuth(context.Background(),
		stubHandler{name: "public", requiresAuth: false})
	if err != nil {
		t.Fatalf("a handler requiring no auth was denied: %v", err)
	}
}

func TestDefaultAuthValidatorDeniesWhenNothingStampedTheKeys(t *testing.T) {
	// The case that matters: a consumer whose authority lives on some other
	// package's context keys. The validator cannot see it, and must deny rather
	// than admit an unjudgeable caller.
	err := DefaultAuthValidator().ValidateExecutionAuth(context.Background(),
		stubHandler{name: "protected", requiresAuth: true})
	if err == nil {
		t.Fatal("a handler requiring auth was ADMITTED on an unstamped context; the gate must fail closed")
	}
}

func TestDefaultAuthValidatorIsWhatAnUnconfiguredServerAlreadyUses(t *testing.T) {
	// Passing it must be equivalent to leaving Config.AuthValidator unset, or the
	// accessor would document one behaviour and deliver another.
	if DefaultAuthValidator() == nil {
		t.Fatal("DefaultAuthValidator returned nil; SetAuthValidator ignores nil, so this would silently mean 'unset'")
	}
	s := &RPCServer{}
	s.SetAuthValidator(DefaultAuthValidator())
	if s.authValidator == nil {
		t.Fatal("SetAuthValidator did not accept the default validator")
	}
	s2 := &RPCServer{authValidator: DefaultAuthValidator()}
	s2.SetAuthValidator(nil)
	if s2.authValidator == nil {
		t.Fatal("a nil SetAuthValidator cleared the validator; nil must be ignored, not applied")
	}
}
