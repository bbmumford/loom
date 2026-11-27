/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpc

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// routeMethods are the HTTP methods an HTTPRoute may name; a route taking its
// Request in the body names one of routeBodyMethods.
var (
	routeMethods = map[string]bool{
		http.MethodGet: true, http.MethodHead: true, http.MethodPost: true,
		http.MethodPut: true, http.MethodPatch: true, http.MethodDelete: true,
	}
	routeBodyMethods = map[string]bool{http.MethodPost: true, http.MethodPut: true, http.MethodPatch: true, http.MethodDelete: true}
)

// routeVariable is a pattern segment that is one {field} variable.
var routeVariable = regexp.MustCompile(`^\{([A-Za-z_][A-Za-z0-9_]*)\}$`)

// routeVariables matches every variable of a pattern, for routeKey.
var routeVariables = regexp.MustCompile(`\{[^}]*\}`)

// RouteVariables returns the field names a pattern's {field} segments name, in
// path order. pkg/rpc/http reads each segment's value by these names.
func RouteVariables(pattern string) []string {
	var names []string
	for _, seg := range strings.Split(strings.TrimPrefix(pattern, "/"), "/") {
		if m := routeVariable.FindStringSubmatch(seg); m != nil {
			names = append(names, m[1])
		}
	}
	return names
}

// RouteField is the field of desc a route variable or query parameter named
// name fills: the field with that proto name, else the one with that JSON
// name; nil when there is none.
func RouteField(desc protoreflect.MessageDescriptor, name string) protoreflect.FieldDescriptor {
	if fd := desc.Fields().ByName(protoreflect.Name(name)); fd != nil {
		return fd
	}
	return desc.Fields().ByJSONName(name)
}

// routeKey is a route as a router matches it: its method and its pattern with
// every variable's name dropped, since two patterns differing only in their
// variables' names match the same requests.
func routeKey(rt HTTPRoute) string {
	return rt.Method + " " + routeVariables.ReplaceAllString(rt.Pattern, "{}")
}

// checkRoutes refuses a route of h the HTTP mounts could not serve as its
// contract states it (HTTPRoute): an unknown method, a body that is neither "*"
// nor "", a body on GET or HEAD, a pattern that is not an absolute path of
// literal and {field} segments, that lies under /rpc (the bridge's own routes),
// that names a field twice, or whose variable names no singular scalar field of
// h's Request; and the same route twice. It answers each route's routeKey.
func checkRoutes(h *Handler) ([]string, error) {
	if len(h.Routes) == 0 {
		return nil, nil
	}
	if h.Request == nil {
		return nil, fmt.Errorf("handler %s declares HTTP routes and no Request", h.FQN())
	}
	desc := h.Request.ProtoReflect().Descriptor()
	keys := make([]string, 0, len(h.Routes))
	for _, rt := range h.Routes {
		refuse := func(why string, args ...any) error {
			return fmt.Errorf("handler %s: route %s %s: %s", h.FQN(), rt.Method, rt.Pattern, fmt.Sprintf(why, args...))
		}
		if !routeMethods[rt.Method] {
			return nil, refuse("the method is not one an HTTP route may name")
		}
		switch {
		case rt.Body != "" && rt.Body != "*":
			return nil, refuse("body %q: a route takes the whole Request (\"*\") or no body (\"\")", rt.Body)
		case rt.Body == "*" && !routeBodyMethods[rt.Method]:
			return nil, refuse("a %s request carries no body", rt.Method)
		}
		if !strings.HasPrefix(rt.Pattern, "/") || rt.Pattern == "/rpc" || strings.HasPrefix(rt.Pattern, "/rpc/") {
			return nil, refuse("the pattern must be an absolute path outside /rpc")
		}
		named := map[string]bool{}
		for _, seg := range strings.Split(rt.Pattern[1:], "/") {
			if seg == "" {
				return nil, refuse("the pattern has an empty segment")
			}
			if !strings.ContainsAny(seg, "{}*:") {
				continue
			}
			m := routeVariable.FindStringSubmatch(seg)
			if m == nil {
				return nil, refuse("segment %q: a segment is a literal or one {field}", seg)
			}
			if named[m[1]] {
				return nil, refuse("{%s} appears twice", m[1])
			}
			named[m[1]] = true
			fd := RouteField(desc, m[1])
			switch {
			case fd == nil:
				return nil, refuse("{%s} names no field of %s", m[1], desc.FullName())
			case fd.IsList() || fd.IsMap() || fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
				return nil, refuse("{%s} names %s, which is not a singular scalar field", m[1], fd.FullName())
			}
		}
		key := routeKey(rt)
		for _, k := range keys {
			if k == key {
				return nil, refuse("the handler declares the route twice")
			}
		}
		keys = append(keys, key)
	}
	return keys, nil
}
