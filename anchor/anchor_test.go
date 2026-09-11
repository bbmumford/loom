package anchor

import (
	"context"
	"testing"

	"github.com/bbmumford/loom/anchor/pb"
	"github.com/bbmumford/loom/pkg/rpc"
)

func TestPolicyVerify(t *testing.T) {
	fly := Policy{PrivateAddressPrefix: "fdaa:"}
	cases := []struct {
		name  string
		p     Policy
		req   *pb.VerifyBootstrapRequest
		allow bool
	}{
		{"well-formed node", fly, &pb.VerifyBootstrapRequest{NodeId: "vl1_abc", ServiceName: "api", PrivateIp: "fdaa:0:1::2"}, true},
		{"no private address reported", fly, &pb.VerifyBootstrapRequest{NodeId: "vl1_abc", ServiceName: "api"}, true},
		{"missing node id", fly, &pb.VerifyBootstrapRequest{ServiceName: "api"}, false},
		{"missing service name", fly, &pb.VerifyBootstrapRequest{NodeId: "vl1_abc"}, false},
		{"node id without the prefix", fly, &pb.VerifyBootstrapRequest{NodeId: "node_abc", ServiceName: "api"}, false},
		{"private address outside the prefix", fly, &pb.VerifyBootstrapRequest{NodeId: "vl1_abc", ServiceName: "api", PrivateIp: "10.0.0.2"}, false},
		{"no address prefix configured", Policy{}, &pb.VerifyBootstrapRequest{NodeId: "vl1_abc", ServiceName: "api", PrivateIp: "10.0.0.2"}, true},
		{"custom node id prefix", Policy{NodeIDPrefix: "n_"}, &pb.VerifyBootstrapRequest{NodeId: "vl1_abc", ServiceName: "api"}, false},
	}
	for _, c := range cases {
		resp := c.p.Verify(c.req)
		if resp.GetAllowed() != c.allow {
			t.Errorf("%s: allowed=%v (reason %q), want %v", c.name, resp.GetAllowed(), resp.GetReason(), c.allow)
		}
		if !resp.GetAllowed() && resp.GetReason() == "" {
			t.Errorf("%s: a refusal must say why", c.name)
		}
	}
}

// TestRegisterServesThePolicyUnderTheFQNClientCalls wires Register into a
// registry and dispatches through the registered handler, so the FQN Client
// calls and the one the anchor-capable node answers cannot drift apart.
func TestRegisterServesThePolicyUnderTheFQNClientCalls(t *testing.T) {
	reg := rpc.NewRegistry()
	if err := Register(Policy{PrivateAddressPrefix: "fdaa:"})(reg); err != nil {
		t.Fatal(err)
	}
	h, ok := reg.Get(VerifyBootstrapFQN)
	if !ok || h.Func == nil {
		t.Fatalf("no handler registered under %q", VerifyBootstrapFQN)
	}
	out, err := h.Func(context.Background(), &pb.VerifyBootstrapRequest{NodeId: "vl1_abc", ServiceName: "api", PrivateIp: "10.0.0.2"})
	if err != nil {
		t.Fatal(err)
	}
	if resp, _ := out.(*pb.VerifyBootstrapResponse); resp == nil || resp.GetAllowed() {
		t.Fatalf("the registered handler must apply the policy it was given; got %+v", out)
	}

	types := Types()
	if len(types) != 1 || types[0].FQN() != VerifyBootstrapFQN || types[0].Func != nil {
		t.Fatalf("Types() must list the same operation without its Func; got %+v", types)
	}
}
