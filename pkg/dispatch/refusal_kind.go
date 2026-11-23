/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package dispatch

import "github.com/ORBTR/aether/rpc/pb"

// StampFailed records on resp, a failed response, the one refusal kind err
// carries: StampRefusal's when err is a typed route refusal or an ambiguous
// outcome, else StampDenial's when it refuses the caller's authority, else
// StampConflict's when it is a conflict with stored state, with StampCoded's
// status and code beside either or alone when err carries a handler's coded
// refusal. Anything else leaves resp as it is, and the caller reads it as a
// handler rejection. node's
// serveHandlerStream stamps the failed answer to a stream it refuses with it, so
// OpenHandlerStream rebuilds the typed error (responseError) as HWPCaller does
// for a refused call, and RefusalKind names the kind with it.
func StampFailed(resp *pb.RPCResponse, err error) {
	if resp == nil || resp.Success || err == nil {
		return
	}
	StampRefusal(resp, err)
	if resp.Metadata[refusalKindKey] != "" {
		return
	}
	StampDenial(resp, err)
	StampConflict(resp, err)
	StampCoded(resp, err)
}

// RefusalKind is the kind a failed response carrying err is stamped with on the
// mesh (StampFailed): "denied", "conflict", "grade", "deadline", "ambiguous",
// "adverts", "no-answerer" or another kind StampRefusal writes, and "" for an error
// no stamp records. pkg/rpc/http writes it into a stream's error event beside the
// HTTP status the same error answers a unary call with, so a client reads one
// vocabulary for a refusal however it arrives.
func RefusalKind(err error) string {
	resp := &pb.RPCResponse{}
	StampFailed(resp, err)
	return resp.Metadata[refusalKindKey]
}
