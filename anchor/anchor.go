// Package anchor is the mesh's bootstrap admission check. When a peer is about
// to admit a node over VL1, it asks an anchor-capable node whether the joining
// node may bootstrap: Client is loom's node.Config.VerifyBootstrap seam, and an
// anchor-capable node answers through the handler Register installs. The check
// is on the form of what the joining node reports; the Noise handshake it makes
// next is what authenticates it.
package anchor

import (
	"context"
	"fmt"
	"strings"

	"github.com/bbmumford/loom/anchor/pb"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/ports"
)

// VerifyBootstrapFQN is the operation an anchor-capable node serves and Client calls.
const VerifyBootstrapFQN = "loom.anchor.VerifyBootstrap"

// defaultNodeIDPrefix is the prefix every VL1 node id carries.
const defaultNodeIDPrefix = "vl1_"

// Policy is what an anchor-capable node requires of a joining node.
type Policy struct {
	// NodeIDPrefix the joining node's id must start with. Empty ⇒ "vl1_".
	NodeIDPrefix string
	// PrivateAddressPrefix, when set, is required of a reported private
	// address, e.g. "fdaa:" for Fly's private network. A node that reports no
	// private address is not refused for it.
	PrivateAddressPrefix string
}

// Verify decides one bootstrap request. It refuses a missing node id or service
// name, a node id without the required prefix, and a reported private address
// outside the required prefix.
func (p Policy) Verify(req *pb.VerifyBootstrapRequest) *pb.VerifyBootstrapResponse {
	if req.GetNodeId() == "" {
		return &pb.VerifyBootstrapResponse{Allowed: false, Reason: "missing node_id"}
	}
	if req.GetServiceName() == "" {
		return &pb.VerifyBootstrapResponse{Allowed: false, Reason: "missing service_name"}
	}
	prefix := p.NodeIDPrefix
	if prefix == "" {
		prefix = defaultNodeIDPrefix
	}
	if !strings.HasPrefix(req.GetNodeId(), prefix) {
		id := req.GetNodeId()
		return &pb.VerifyBootstrapResponse{
			Allowed: false,
			Reason:  fmt.Sprintf("invalid node_id format: %s", id[:min(len(id), 20)]),
		}
	}
	if p.PrivateAddressPrefix != "" && req.GetPrivateIp() != "" && !strings.HasPrefix(req.GetPrivateIp(), p.PrivateAddressPrefix) {
		return &pb.VerifyBootstrapResponse{
			Allowed: false,
			Reason:  fmt.Sprintf("private_ip must start with %s", p.PrivateAddressPrefix),
		}
	}
	return &pb.VerifyBootstrapResponse{Allowed: true}
}

// handlers is the one list Register and Types both read.
func handlers(p Policy) []rpc.Handler {
	return []rpc.Handler{{
		Namespace: "loom", Domain: "anchor", Operation: "VerifyBootstrap",
		Func: rpc.Wrap(func(_ context.Context, req *pb.VerifyBootstrapRequest) (*pb.VerifyBootstrapResponse, error) {
			return p.Verify(req), nil
		}),
		Request:  (*pb.VerifyBootstrapRequest)(nil),
		Response: (*pb.VerifyBootstrapResponse)(nil),
		Scope:    rpc.ScopeNone,
	}}
}

// Register returns the registration an anchor-capable node lists in
// node.Config.RegisterDomains, answering VerifyBootstrap under p.
func Register(p Policy) func(*rpc.Registry) error {
	return func(reg *rpc.Registry) error {
		return reg.RegisterAll(handlers(p))
	}
}

// Types returns the handler metadata without Func, for a node that forwards the
// operation instead of answering it.
func Types() []rpc.Handler {
	return rpc.StripFuncs(handlers(Policy{}))
}

// Client asks an anchor-capable node over the mesh whether the node described by
// info may bootstrap. loom's VL1 bootstrap handler calls it on every node that is
// not itself anchor-capable, and admits the node when the call fails.
func Client(ctx context.Context, info ports.BootstrapInfo) (bool, string, error) {
	resp, err := rpc.Call[*pb.VerifyBootstrapResponse](ctx, VerifyBootstrapFQN, &pb.VerifyBootstrapRequest{
		NodeId:      string(info.NodeID),
		ServiceName: info.ServiceName,
		Region:      info.Region,
		Roles:       info.Roles,
		PrivateIp:   info.PrivateIP,
		PublicIp:    info.PublicIP,
	})
	if err != nil {
		return false, "", err
	}
	return resp.GetAllowed(), resp.GetReason(), nil
}

var _ ports.VerifyBootstrapFunc = Client
