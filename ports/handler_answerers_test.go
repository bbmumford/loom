/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"reflect"
	"testing"
)

func advert(node NodeID, required ...string) HandlerAdvert {
	return HandlerAdvert{Name: "orbtr.audit.Write", NodeID: node, RequiredScopes: required}
}

// Several adverts naming one requirement all answer, whichever order they arrive
// in, because a second answerer is how the mesh carries load. Only what they
// require is compared; their number is not.
func TestSeveralAdvertsNamingOneRequirementAllAnswer(t *testing.T) {
	got := JudgeHandlerAdverts([]HandlerAdvert{
		advert("n2", "capability:orbtr:audit.write"),
		advert("n1", "capability:orbtr:audit.write"),
		advert("n3", "capability:orbtr:audit.write"),
	})
	if got.Disagree() || !reflect.DeepEqual(got.Answerers, []NodeID{"n1", "n2", "n3"}) || got.Undeclared != nil {
		t.Fatalf("judged = %+v; want all three answering in NodeID order", got)
	}
}

// One advert requiring nothing beside one that requires a capability gives no
// answerer at all: a router picking by latency could otherwise take the call to
// the open one, and nothing here can say which advert is right.
func TestAnAdvertRequiringLessLeavesNoAnswerer(t *testing.T) {
	got := JudgeHandlerAdverts([]HandlerAdvert{advert("n1", "capability:orbtr:audit.write"), advert("n2", "open")})
	if !got.Disagree() || len(got.Answerers) != 0 {
		t.Fatalf("judged = %+v; want disagreement and no answerer", got)
	}
	want := map[string][]NodeID{"capability:orbtr:audit.write": {"n1"}, "open": {"n2"}}
	if !reflect.DeepEqual(got.Requirements, want) {
		t.Fatalf("requirements = %v, want %v", got.Requirements, want)
	}
}

// A node naming no requirement does not answer beside a node that names one, and
// does not stop that node answering: it holds nothing the other has declared.
func TestAnAdvertNamingNothingDoesNotAnswerBesideOneThatDoes(t *testing.T) {
	got := JudgeHandlerAdverts([]HandlerAdvert{advert("n1", "nexus"), advert("n2")})
	if got.Disagree() || !reflect.DeepEqual(got.Answerers, []NodeID{"n1"}) || !reflect.DeepEqual(got.Undeclared, []NodeID{"n2"}) {
		t.Fatalf("judged = %+v; want n1 answering and n2 undeclared", got)
	}
}

// No advert naming a requirement leaves nothing to compare, so all stand: a
// fleet whose records carry no requirement routes as its adverts allow.
func TestAdvertsNamingNothingAllStand(t *testing.T) {
	got := JudgeHandlerAdverts([]HandlerAdvert{advert("n2"), advert("n1")})
	if got.Disagree() || !reflect.DeepEqual(got.Answerers, []NodeID{"n1", "n2"}) || got.Undeclared != nil {
		t.Fatalf("judged = %+v; want both standing", got)
	}
}

// The order a node lists several scopes in is not a difference between nodes.
func TestScopeOrderIsNotADisagreement(t *testing.T) {
	got := JudgeHandlerAdverts([]HandlerAdvert{advert("n1", "a", "b"), advert("n2", "b", "a")})
	if got.Disagree() || len(got.Answerers) != 2 {
		t.Fatalf("judged = %+v; want agreement", got)
	}
}
