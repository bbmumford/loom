/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"net/url"
	"strconv"
	"strings"
)

// HandlerRequirementsTag is the peer record tag that names what each listed
// operation asks of a caller. node's PeerPublisher writes it beside RpcHandlers;
// node.peerRecordToRoleRecord and SwarmDirectory read it back.
//
// The value is the distinct requirements, then runs over RpcHandlers in the order
// the record lists them: "<req>,<req>;<index>x<count>,...". Every field is escaped,
// so no requirement can hold a separator. The record is one signed swarm body held
// under swarm.Config.MaxRecordBytes with every FQN already on it, and operations of
// one domain sit next to each other in that sorted list and mostly ask the same
// thing, so a run costs a few bytes where repeating each FQN would cost the record
// a second copy of its largest field.
const HandlerRequirementsTag = "handler_requires"

// EncodeHandlerRequirements renders the tag value for requirements listed in the
// order of RpcHandlers. An empty list renders "".
func EncodeHandlerRequirements(requirements []string) string {
	if len(requirements) == 0 {
		return ""
	}
	index := map[string]int{}
	var table, runs []string
	last, count := -1, 0
	flush := func() {
		if count > 0 {
			runs = append(runs, strconv.Itoa(last)+"x"+strconv.Itoa(count))
		}
	}
	for _, r := range requirements {
		i, seen := index[r]
		if !seen {
			i = len(table)
			index[r] = i
			table = append(table, url.QueryEscape(r))
		}
		if i != last {
			flush()
			last, count = i, 0
		}
		count++
	}
	flush()
	return strings.Join(table, ",") + ";" + strings.Join(runs, ",")
}

// DecodeHandlerRequirements returns the requirement of each of handlers listed
// operations, in their order, or nil when value does not cover exactly that many.
// A value that does not parse names nothing: ports.JudgeHandlerAdverts treats such
// a node as naming no requirement, which keeps it from answering beside a node
// that names one.
func DecodeHandlerRequirements(value string, handlers int) []string {
	tablePart, runsPart, ok := strings.Cut(value, ";")
	if !ok || handlers <= 0 {
		return nil
	}
	var table []string
	for _, raw := range strings.Split(tablePart, ",") {
		r, err := url.QueryUnescape(raw)
		if err != nil {
			return nil
		}
		table = append(table, r)
	}
	out := make([]string, 0, handlers)
	for _, run := range strings.Split(runsPart, ",") {
		i, n, ok := strings.Cut(run, "x")
		if !ok {
			return nil
		}
		idx, err := strconv.Atoi(i)
		if err != nil || idx < 0 || idx >= len(table) {
			return nil
		}
		count, err := strconv.Atoi(n)
		if err != nil || count <= 0 || count > handlers-len(out) {
			return nil
		}
		for j := 0; j < count; j++ {
			out = append(out, table[idx])
		}
	}
	if len(out) != handlers {
		return nil
	}
	return out
}

// HandlerRequirements finds the tag among a peer record's tags and decodes it for
// a record listing handlers operations. nil when the record carries no such tag.
func HandlerRequirements(tags []string, handlers int) []string {
	for _, tag := range tags {
		if k, v, ok := strings.Cut(tag, "="); ok && k == HandlerRequirementsTag {
			return DecodeHandlerRequirements(v, handlers)
		}
	}
	return nil
}
