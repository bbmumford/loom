package rpc

import (
	"context"
	"fmt"

	sharedDispatch "github.com/bbmumford/loom/pkg/dispatch"
)

// OpenNodeStream opens a handler stream to exactly nodeID: frames both ways
// with the StreamHandler handler names there, the caller's sealed principal
// judged as an RPC's is. Same contract as CallNodeRaw: an unreachable target is
// ErrNoRouteToNode, never another node.
func OpenNodeStream(ctx context.Context, nodeID, handler string) (sharedDispatch.NodeStream, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("stream %s: empty nodeID: %w", handler, sharedDispatch.ErrNoRouteToNode)
	}
	caller := sharedDispatch.GetCaller()
	if caller == nil {
		return nil, fmt.Errorf("dispatch caller not initialized")
	}
	streamer, ok := caller.(sharedDispatch.NodeStreamer)
	if !ok {
		return nil, fmt.Errorf("stream %s to node %s: caller %T opens no handler stream: %w",
			handler, nodeID, caller, sharedDispatch.ErrNoRouteToNode)
	}
	return streamer.OpenNodeStream(ctx, nodeID, ExtractRole(handler), handler)
}
