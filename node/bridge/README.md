# node/bridge

This directory holds no Go code. A loom node has no HTTP gateway role and no
REST bridge of its own; the two paths an HTTP request or a cross-node call
takes are these.

## HTTP into a node's handlers

`pkg/rpc/http` (`rpchttp.Bridge`) serves every operation in an `rpc.Registry`
over HTTP. `rpchttp.RegisterAll` (or `RegisterAllWithBridge`, for a bridge with
its own identity injector and directory authorisation) mounts each handler at
`POST /rpc/<fqn>` and the handler directory at `GET /rpc/_directory`.

A handler registered with `RegisterProxy` has no `Func` on this node: the bridge
forwards it over the mesh to the node that executes it.

## Calls between nodes

`rpc.Call` and `rpc.CallRaw` (`pkg/rpc/call.go`) dispatch locally when this
node executes the handler and otherwise over the mesh. The serving node is
found from the fleet.peer records nodes publish (`node.RoleTable`), which carry
only the handlers each node executes. `rpc.CallNode` and `rpc.CallNodeRaw`
target one node by ID.

## See also

- `pkg/rpc/README.md` for the registry, scopes and the HTTP bridge.
- `node/RUNTIME_README.md` for the runtime that serves mesh RPC.
