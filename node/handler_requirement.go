/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"fmt"
	"sort"
	"strings"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

// requirementTag returns the peer record tag naming what each executed handler in
// registered asks of a caller: those with a Func, in the order publishOnce lists
// them in RpcHandlers, which the tag's runs index. "" when none is executed.
func requirementTag(registered []*rpc.Handler) string {
	var requirements []string
	for _, h := range registered {
		if h.Func == nil {
			continue
		}
		requirements = append(requirements, scope.AdvertisedRequirement(h.Requirement, h.Scope))
	}
	if len(requirements) == 0 {
		return ""
	}
	return directory.HandlerRequirementsTag + "=" + directory.EncodeHandlerRequirements(requirements)
}

var _ directory.HandlerAdvertSource = (*RoleTable)(nil)

// handlerAdverts projects the records Lookup returned for handler onto the port's
// advert type, the input ports.JudgeHandlerAdverts compares.
func handlerAdverts(records []lad.RoleRecord, handler string) []ports.HandlerAdvert {
	out := make([]ports.HandlerAdvert, 0, len(records))
	for _, r := range records {
		advert := ports.HandlerAdvert{Name: handler, NodeID: ports.NodeID(r.NodeID)}
		for _, h := range r.Handlers {
			if h.Name == handler {
				advert.RequiredScopes = h.RequiredScopes
				break
			}
		}
		out = append(out, advert)
	}
	return out
}

// HandlerAdverts implements directory.HandlerAdvertSource: every peer advertising
// name, with what it requires, so LADDirectory.HandlersByName answers from the
// records routing picks among.
func (t *RoleTable) HandlerAdverts(name string) []ports.HandlerAdvert {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []ports.HandlerAdvert
	for id, rec := range t.byNode {
		for _, h := range rec.Handlers {
			if h.Name != name {
				continue
			}
			out = append(out, ports.HandlerAdvert{
				Name:           name,
				NodeID:         ports.NodeID(id),
				Roles:          append([]string(nil), rec.Roles...),
				RequiredScopes: append([]string(nil), h.RequiredScopes...),
			})
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// HandlerNames implements directory.HandlerAdvertSource: every name a peer
// advertises.
func (t *RoleTable) HandlerNames() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	set := map[string]bool{}
	for _, rec := range t.byNode {
		for _, h := range rec.Handlers {
			set[h.Name] = true
		}
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Answerers returns the peers a call to handler may reach: Lookup's records for
// role and handler, without any whose advert names no requirement beside an advert
// that names one. It returns an *dispatch.AnswerersDisagreeError when the adverts
// name different requirements. An empty handler names no operation to compare, and
// returns every peer of the role.
//
// findNodeForRole and the forwarder pick only from what it returns, because each
// picks by latency or list order and neither looks at what a peer requires: the
// peer that asks least would take the call as readily as the one that asks most.
// Several peers naming one requirement all stand, which is what lets more than one
// node carry an operation.
func (t *RoleTable) Answerers(role, handler string) ([]lad.RoleRecord, error) {
	records := t.Lookup(role, handler)
	if handler == "" || len(records) < 2 {
		return records, nil
	}
	judged := ports.JudgeHandlerAdverts(handlerAdverts(records, handler))
	if judged.Disagree() {
		return nil, answerersDisagree(handler, judged)
	}
	if len(judged.Undeclared) == 0 {
		return records, nil
	}
	kept := make([]lad.RoleRecord, 0, len(judged.Answerers))
	for _, r := range records {
		if nodeListed(judged.Answerers, r.NodeID) {
			kept = append(kept, r)
		}
	}
	return kept, nil
}

// CheckAnswerers implements dispatch.AnswererChecker for HWPCaller: nil when a call
// to handler may leave, and the typed refusal when the adverts of handler disagree
// or when nodeID, the node a targeted call names, is one whose advert names no
// requirement beside adverts that do. A node with no swarm running holds no adverts
// to compare, so it refuses nothing here.
func (m *ConnectionManager) CheckAnswerers(role, handler, nodeID string) error {
	if m == nil || m.rt == nil || m.rt.swarm == nil || m.rt.swarm.RoleTable == nil {
		return nil
	}
	records := m.rt.swarm.RoleTable.Lookup(role, handler)
	if len(records) < 2 {
		return nil
	}
	judged := ports.JudgeHandlerAdverts(handlerAdverts(records, handler))
	if judged.Disagree() {
		return answerersDisagree(handler, judged)
	}
	if nodeID != "" && nodeListed(judged.Undeclared, nodeID) {
		return answerersDisagree(handler, judged)
	}
	return nil
}

// answerersDisagree renders a judgement as the typed refusal, with its groups in a
// fixed order so one fleet state always reads the same in a log.
func answerersDisagree(handler string, judged ports.HandlerAnswerers) error {
	groups := make([]string, 0, len(judged.Requirements)+1)
	for requirement, nodes := range judged.Requirements {
		groups = append(groups, requirementGroup(requirement, nodes))
	}
	if len(judged.Undeclared) > 0 {
		groups = append(groups, requirementGroup("none named", judged.Undeclared))
	}
	sort.Strings(groups)
	return &dispatch.AnswerersDisagreeError{Handler: handler, Requirements: groups}
}

func requirementGroup(requirement string, nodes []ports.NodeID) string {
	short := make([]string, 0, len(nodes))
	for _, n := range nodes {
		short = append(short, truncID(string(n)))
	}
	return fmt.Sprintf("%s: %s", requirement, strings.Join(short, ","))
}

func nodeListed(nodes []ports.NodeID, id string) bool {
	for _, n := range nodes {
		if string(n) == id {
			return true
		}
	}
	return false
}
