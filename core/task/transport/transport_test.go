/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package transport

import (
	"context"
	"testing"
	"time"

	task "github.com/bbmumford/loom/core/task"
	"github.com/bbmumford/loom/core/task/gateway"
)

func TestBindGatewayTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gw, err := gateway.NewGateway(gateway.GatewayConfig{ID: "gw-test"})
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	gw.Start(ctx)
	defer gw.Stop()

	transport := newMockTransport()
	binding := BindGateway(ctx, gw, transport)

	// Enqueue a task and ensure an offer reaches the transport.
	testTask := &task.Task{
		ID:       "task-1",
		TenantID: "tenant",
		Role:     "role",
		Type:     "demo",
		SLO:      task.TaskSLO{},
	}

	if err := gw.Enqueue(testTask); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var offer task.TaskOffer
	select {
	case offer = <-transport.offers:
	case <-time.After(2 * time.Second):
		t.Fatalf("offer not forwarded")
	}
	if offer.Task == nil || offer.Task.ID != testTask.ID {
		t.Fatalf("unexpected offer payload")
	}

	// Simulate a winning bid and ensure assignment is forwarded.
	transport.bids <- task.Bid{TaskID: testTask.ID, NodeID: "node-1", CapacityScore: 1, ETA: 100 * time.Millisecond, ReceivedAt: time.Now()}

	var assignment task.Assignment
	select {
	case assignment = <-transport.assignments:
	case <-time.After(2 * time.Second):
		t.Fatalf("assignment not forwarded")
	}
	if assignment.TaskID != testTask.ID {
		t.Fatalf("unexpected assignment task id: %s", assignment.TaskID)
	}

	// Deliver a successful result back to the gateway.
	transport.results <- task.ResultReport{Result: task.Result{TaskID: testTask.ID, NodeID: assignment.Primary, Fence: assignment.Fence, Status: task.ResultStatusOK}}

	// Observe completion through the gateway, NOT by reading testTask
	// directly. The result loop writes Status on its own goroutine under the
	// gateway's lock; a bare read here is a data race (measured: caught by
	// -race 1 run in 10), and sleeping does not create a happens-before edge.
	var status task.TaskStatus
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if status = gw.StatusOf(testTask); status == task.TaskStatusCompleted {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if status != task.TaskStatusCompleted {
		t.Fatalf("task status not updated, got %s", status)
	}

	// Shutdown binding and check transport closed.
	binding.Stop()
	binding.Wait()

	select {
	case <-transport.closed:
	default:
		t.Fatalf("transport not closed")
	}

	for err := range binding.Errors() {
		if err != nil {
			t.Fatalf("binding reported error: %v", err)
		}
	}
}

type mockTransport struct {
	offers      chan task.TaskOffer
	assignments chan task.Assignment
	bids        chan task.Bid
	results     chan task.ResultReport
	closed      chan struct{}
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		offers:      make(chan task.TaskOffer, 4),
		assignments: make(chan task.Assignment, 4),
		bids:        make(chan task.Bid, 4),
		results:     make(chan task.ResultReport, 4),
		closed:      make(chan struct{}),
	}
}

func (m *mockTransport) PublishOffer(ctx context.Context, offer task.TaskOffer) error {
	select {
	case m.offers <- offer:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mockTransport) PublishAssignment(ctx context.Context, assignment task.Assignment) error {
	select {
	case m.assignments <- assignment:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mockTransport) BidStream() <-chan task.Bid { return m.bids }

func (m *mockTransport) ResultStream() <-chan task.ResultReport { return m.results }

func (m *mockTransport) ProgressStream() <-chan task.Progress { return nil }

func (m *mockTransport) Close() error {
	select {
	case <-m.closed:
	default:
		close(m.closed)
	}
	return nil
}
