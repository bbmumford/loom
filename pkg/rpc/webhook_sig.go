/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ErrWebhookSignatureInvalid is the sentinel returned when a webhook-class
// handler's pre-dispatch signature verification fails. Callers can
// errors.Is at HTTP/wire boundaries (→ 401).
var ErrWebhookSignatureInvalid = errors.New("rpc: webhook signature verification failed")

// ErrWebhookSecretUnset is returned when WebhookSignature is declared but
// the configured SecretEnvVar resolves to an empty string. Fail-closed by
// default — production deployments MUST populate the env var so the
// dispatch path itself is protected (gateway-side verification alone
// leaves the mesh path spoofable by any peer with mesh access).
var ErrWebhookSecretUnset = errors.New("rpc: webhook secret unset")

// WebhookSignatureConfig declares that a handler is a webhook-class
// receiver whose payload must carry a verifiable signature header. The
// registry runs the signature check BEFORE invoking handler.Func so the
// handler body never sees an unauthenticated event.
//
// The request proto MUST expose `payload` and `signature` string fields
// (lowercase proto field names). The registry uses protoreflect to read
// them off the unmarshaled message — no per-handler boilerplate.
//
// Algorithms are looked up in the package-level verifier registry; see
// RegisterWebhookVerifier for adding new algorithms.
type WebhookSignatureConfig struct {
	// Algorithm picks the verifier from the registry. Built-in:
	// "stripe-hmac-sha256". Add more via RegisterWebhookVerifier.
	Algorithm string
	// SecretEnvVar names the OS env var that holds the signing secret.
	// Resolved at dispatch time (NOT registration time) so secrets can
	// rotate without redeploy. Ignored if SecretProvider is non-nil.
	SecretEnvVar string
	// SecretProvider is an optional override that returns the current
	// signing secret. Use this when the secret is sourced from a config
	// loader (e.g. cfgload) rather than the process env. Called on every
	// dispatch — implementations should be cheap (read a cached field).
	SecretProvider func() string
	// Header names the wire header (e.g. "Stripe-Signature"). Currently
	// informational — the signature value is read off the request proto's
	// `signature` field by the HTTP bridge before dispatch. Kept on the
	// config so domains, docs, and gateways agree on the canonical header.
	Header string
	// MaxSkew bounds the acceptable age of the signature timestamp.
	// Zero means "use the verifier's documented default" (Stripe: 5min).
	MaxSkew time.Duration
}

// WebhookVerifier is the function signature for an algorithm verifier.
// It receives the payload bytes, the signature header value, the
// resolved secret, and the configured max skew. Returns nil on success
// or an error wrapping ErrWebhookSignatureInvalid on any mismatch.
type WebhookVerifier func(payload, signatureHeader, secret string, maxSkew time.Duration) error

var (
	webhookVerifiersMu sync.RWMutex
	webhookVerifiers   = map[string]WebhookVerifier{
		"stripe-hmac-sha256": VerifyStripeHMACSHA256,
	}
)

// RegisterWebhookVerifier adds a verifier under the given algorithm name.
// Domains that need a new scheme (GitHub, Twilio, etc.) call this at init
// time. Re-registering the same algorithm overwrites — tests rely on this.
func RegisterWebhookVerifier(algorithm string, verifier WebhookVerifier) {
	if algorithm == "" || verifier == nil {
		return
	}
	webhookVerifiersMu.Lock()
	defer webhookVerifiersMu.Unlock()
	webhookVerifiers[algorithm] = verifier
}

// lookupWebhookVerifier returns the verifier for the given algorithm.
func lookupWebhookVerifier(algorithm string) (WebhookVerifier, bool) {
	webhookVerifiersMu.RLock()
	defer webhookVerifiersMu.RUnlock()
	v, ok := webhookVerifiers[algorithm]
	return v, ok
}

// VerifyWebhookSignature is the exported entry point for out-of-package
// callers (e.g. rpchttp.Bridge.executeLocal) that need to run the same
// pre-dispatch signature check Registry.Dispatch runs. It delegates to
// the unexported verifyWebhookSignature and intentionally shares its
// fail-closed semantics so the HTTP bridge and the server-side dispatch
// path can't diverge on what counts as a valid signed event.
func VerifyWebhookSignature(cfg *WebhookSignatureConfig, req proto.Message) error {
	return verifyWebhookSignature(cfg, req)
}

// verifyWebhookSignature is the pre-dispatch hook the registry runs for
// any handler that declared WebhookSignature. It extracts the payload +
// signature off the unmarshaled request proto via protoreflect, resolves
// the secret from the env var, picks the verifier by algorithm, and
// runs it. Fails closed on every error path — including a missing env
// var — so a misconfigured deployment cannot accidentally accept
// unsigned events.
func verifyWebhookSignature(cfg *WebhookSignatureConfig, req proto.Message) error {
	if cfg == nil {
		return nil
	}
	if cfg.Algorithm == "" {
		return fmt.Errorf("%w: handler webhook config missing Algorithm", ErrWebhookSignatureInvalid)
	}
	verifier, ok := lookupWebhookVerifier(cfg.Algorithm)
	if !ok {
		return fmt.Errorf("%w: unknown algorithm %q", ErrWebhookSignatureInvalid, cfg.Algorithm)
	}

	secret := ""
	switch {
	case cfg.SecretProvider != nil:
		secret = cfg.SecretProvider()
	case cfg.SecretEnvVar != "":
		secret = os.Getenv(cfg.SecretEnvVar)
	}
	if secret == "" {
		if cfg.SecretProvider != nil {
			return fmt.Errorf("%w: provider returned empty", ErrWebhookSecretUnset)
		}
		return fmt.Errorf("%w: env var %q is empty", ErrWebhookSecretUnset, cfg.SecretEnvVar)
	}

	payload, signature, err := extractWebhookFields(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrWebhookSignatureInvalid, err)
	}

	skew := cfg.MaxSkew
	// Verifier interprets skew==0 as "use my default" — keep that contract
	// so callers don't have to hard-code the per-algorithm window here.
	return verifier(payload, signature, secret, skew)
}

// extractWebhookFields reads the `payload` and `signature` string fields
// off the unmarshaled request proto via protoreflect. Avoids forcing
// every webhook request type to implement a Go interface — the proto
// schema is the contract.
func extractWebhookFields(req proto.Message) (payload, signature string, err error) {
	if req == nil {
		return "", "", errors.New("nil request")
	}
	msg := req.ProtoReflect()
	fields := msg.Descriptor().Fields()

	payloadField := fields.ByName(protoreflect.Name("payload"))
	if payloadField == nil {
		return "", "", errors.New("request proto missing 'payload' field")
	}
	signatureField := fields.ByName(protoreflect.Name("signature"))
	if signatureField == nil {
		return "", "", errors.New("request proto missing 'signature' field")
	}
	if payloadField.Kind() != protoreflect.StringKind {
		return "", "", errors.New("request proto 'payload' field is not a string")
	}
	if signatureField.Kind() != protoreflect.StringKind {
		return "", "", errors.New("request proto 'signature' field is not a string")
	}
	payload = msg.Get(payloadField).String()
	signature = msg.Get(signatureField).String()
	if payload == "" {
		return "", "", errors.New("empty payload")
	}
	if signature == "" {
		return "", "", errors.New("empty signature")
	}
	return payload, signature, nil
}

// VerifyStripeHMACSHA256 parses Stripe's `t=…,v1=…` signature header and
// verifies HMAC-SHA256 over `<timestamp>.<payload>` against the configured
// signing secret. Constant-time comparison defends against timing
// oracles. Returns an error wrapping ErrWebhookSignatureInvalid on any
// mismatch or malformed input.
//
// maxSkew==0 is treated as Stripe's documented 5-minute tolerance.
// Negative values disable the timestamp check (intended for tests only —
// production callers MUST leave the default in place).
func VerifyStripeHMACSHA256(payload, signatureHeader, secret string, maxSkew time.Duration) error {
	if secret == "" || signatureHeader == "" {
		return ErrWebhookSignatureInvalid
	}
	if maxSkew == 0 {
		maxSkew = 5 * time.Minute
	}

	var timestamp int64
	var sigs []string
	for _, part := range strings.Split(signatureHeader, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			if ts, err := strconv.ParseInt(kv[1], 10, 64); err == nil {
				timestamp = ts
			}
		case "v1":
			sigs = append(sigs, kv[1])
		}
	}
	if timestamp == 0 || len(sigs) == 0 {
		return ErrWebhookSignatureInvalid
	}
	if maxSkew > 0 {
		age := time.Since(time.Unix(timestamp, 0))
		if age > maxSkew || age < -maxSkew {
			return fmt.Errorf("%w: timestamp out of tolerance (age=%s)", ErrWebhookSignatureInvalid, age)
		}
	}

	signedPayload := fmt.Sprintf("%d.%s", timestamp, payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signedPayload))
	expected := hex.EncodeToString(mac.Sum(nil))

	for _, candidate := range sigs {
		// hmac.Equal is constant-time over equal-length inputs.
		if len(candidate) == len(expected) && hmac.Equal([]byte(candidate), []byte(expected)) {
			return nil
		}
	}
	return ErrWebhookSignatureInvalid
}
