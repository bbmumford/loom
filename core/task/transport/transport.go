/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package transport

import (
	"context"
	"fmt"
	"sync"

	task "github.com/bbmumford/loom/core/task"
	"github.com/bbmumford/loom/core/task/gateway"
)

// GatewayTransport bridges the gateway with a transport layer (gRPC, VL1, etc.).
type GatewayTransport interface {
	PublishOffer(ctx context.Context, offer task.TaskOffer) error
	PublishAssignment(ctx context.Context, assignment task.Assignment) error
	BidStream() <-chan task.Bid
	ResultStream() <-chan task.ResultReport
	ProgressStream() <-chan task.Progress
	Close() error
}

// ErrorHandler receives binding errors for observability.
type ErrorHandler func(error)

// BindingOption configures behaviour of BindGateway.
type BindingOption func(*bindingConfig)

type bindingConfig struct {
	errHandler ErrorHandler
}

// WithBindingErrorHandler registers a callback for errors surfaced by the binding.
func WithBindingErrorHandler(fn ErrorHandler) BindingOption {
	return func(cfg *bindingConfig) {
		cfg.errHandler = fn
	}
}

// GatewayBinding manages the lifecycle of a gateway <-> transport connection.
type GatewayBinding struct {
	cancel     context.CancelFunc
	transport  GatewayTransport
	errHandler ErrorHandler
	errCh      chan error
	wg         sync.WaitGroup
	once       sync.Once
}

// Errors exposes asynchronous errors encountered by the binding.
func (b *GatewayBinding) Errors() <-chan error { return b.errCh }

// Stop terminates the binding and closes the underlying aether.
func (b *GatewayBinding) Stop() {
	b.once.Do(func() {
		if b.cancel != nil {
			b.cancel()
		}
		if b.transport != nil {
			if err := b.transport.Close(); err != nil {
				b.reportError(fmt.Errorf("task: transport close: %w", err))
			}
		}
	})
}

// Wait blocks until all goroutines have completed and the error channel is closed.
func (b *GatewayBinding) Wait() { b.wg.Wait() }

// BindGateway connects a Gateway to a GatewayTransport, wiring outbound offers
// and assignments while feeding inbound bids, progress, and result reports.
func BindGateway(parent context.Context, gw *gateway.Gateway, transport GatewayTransport, opts ...BindingOption) *GatewayBinding {
	if gw == nil {
		panic("task: gateway is nil")
	}
	if transport == nil {
		panic("task: transport is nil")
	}

	cfg := bindingConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	ctx, cancel := context.WithCancel(parent)
	binding := &GatewayBinding{
		cancel:     cancel,
		transport:  transport,
		errHandler: cfg.errHandler,
		errCh:      make(chan error, 16),
	}

	start := func(fn func()) {
		binding.wg.Add(1)
		go func() {
			defer binding.wg.Done()
			fn()
		}()
	}

	start(func() { forwardOffers(ctx, gw, transport, binding.reportError) })
	start(func() { forwardAssignments(ctx, gw, transport, binding.reportError) })
	start(func() { forwardInboundBids(ctx, gw, transport, binding.reportError) })
	start(func() { forwardInboundResults(ctx, gw, transport, binding.reportError) })
	start(func() { forwardInboundProgress(ctx, gw, transport, binding.reportError) })

	go func() {
		<-ctx.Done()
		binding.Stop()
	}()

	go func() {
		binding.wg.Wait()
		binding.Stop()
		close(binding.errCh)
	}()

	return binding
}

func (b *GatewayBinding) reportError(err error) {
	if err == nil {
		return
	}
	if b.errHandler != nil {
		b.errHandler(err)
	}
	defer func() { recover() }()
	select {
	case b.errCh <- err:
	default:
	}
}

func forwardOffers(ctx context.Context, gw *gateway.Gateway, transport GatewayTransport, report ErrorHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		case offer, ok := <-gw.Offers():
			if !ok {
				return
			}
			if err := transport.PublishOffer(ctx, offer); err != nil {
				report(fmt.Errorf("task: publish offer: %w", err))
			}
		}
	}
}

func forwardAssignments(ctx context.Context, gw *gateway.Gateway, transport GatewayTransport, report ErrorHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		case assignment, ok := <-gw.Assignments():
			if !ok {
				return
			}
			if err := transport.PublishAssignment(ctx, assignment); err != nil {
				report(fmt.Errorf("task: publish assignment: %w", err))
			}
		}
	}
}

func forwardInboundBids(ctx context.Context, gw *gateway.Gateway, transport GatewayTransport, report ErrorHandler) {
	bids := transport.BidStream()
	if bids == nil {
		return
	}
	sink := gw.BidSink()
	for {
		select {
		case <-ctx.Done():
			return
		case bid, ok := <-bids:
			if !ok {
				return
			}
			select {
			case sink <- bid:
			case <-ctx.Done():
				return
			}
		}
	}
}

func forwardInboundResults(ctx context.Context, gw *gateway.Gateway, transport GatewayTransport, report ErrorHandler) {
	results := transport.ResultStream()
	if results == nil {
		return
	}
	sink := gw.ResultSink()
	for {
		select {
		case <-ctx.Done():
			return
		case res, ok := <-results:
			if !ok {
				return
			}
			select {
			case sink <- res:
			case <-ctx.Done():
				return
			}
		}
	}
}

func forwardInboundProgress(ctx context.Context, gw *gateway.Gateway, transport GatewayTransport, report ErrorHandler) {
	progress := transport.ProgressStream()
	if progress == nil {
		return
	}
	sink := gw.ProgressSink()
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-progress:
			if !ok {
				return
			}
			select {
			case sink <- p:
			case <-ctx.Done():
				return
			}
		}
	}
}
