/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpchttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// transcoded serves one HTTPRoute of h: it builds the protojson of h's Request
// from the route's path variables and either its query string (a route with no
// body; a parameter names a field, or a field of a singular message field by its
// dotted path) or its body (a route whose body is the whole Request), and hands that to
// dispatch, so the request is parsed, stamped, judged and executed as one sent
// to the operation's /rpc route. A path variable fills its field over the body's
// value for it. A query parameter naming no field, a value its field cannot
// take, a body on a route that takes none, or a query string on a route taking
// the Request in its body is answered 400 before anything is dispatched.
func (b *Bridge) transcoded(h *rpc.Handler, route rpc.HTTPRoute) http.Handler {
	desc := h.Request.ProtoReflect().Descriptor()
	vars := rpc.RouteVariables(route.Pattern)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, b.maxBodySize+1))
		if err != nil {
			writeError(w, http.StatusBadRequest, "read body: "+err.Error())
			return
		}
		if int64(len(body)) > b.maxBodySize {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		fields := map[string]any{}
		sent := len(bytes.TrimSpace(body)) > 0
		switch {
		case route.Body == "*" && r.URL.RawQuery != "":
			writeError(w, http.StatusBadRequest, "this route takes its request in the body; it reads no query string")
			return
		case route.Body == "*" && sent:
			var sentFields map[string]json.RawMessage
			if err := json.Unmarshal(body, &sentFields); err != nil || sentFields == nil {
				writeError(w, http.StatusBadRequest, "invalid request: the body must be a JSON object")
				return
			}
			for k, v := range sentFields {
				fields[k] = v
			}
		case route.Body == "" && sent:
			writeError(w, http.StatusBadRequest, "this route takes no body")
			return
		}
		set := func(fd protoreflect.FieldDescriptor, v json.RawMessage) {
			delete(fields, string(fd.Name()))
			fields[fd.JSONName()] = v
		}
		if route.Body == "" {
			for name, values := range r.URL.Query() {
				at, fd, err := queryField(fields, desc, name)
				if err != nil {
					writeError(w, http.StatusBadRequest, "invalid request: query parameter "+strconv.Quote(name)+": "+err.Error())
					return
				}
				v, err := queryValue(fd, values)
				if err != nil {
					writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
					return
				}
				at[fd.JSONName()] = v
			}
		}
		for _, name := range vars {
			raw, err := url.PathUnescape(chi.URLParam(r, name))
			if err == nil {
				var v json.RawMessage
				if v, err = scalarValue(rpc.RouteField(desc, name), raw); err == nil {
					set(rpc.RouteField(desc, name), v)
					continue
				}
			}
			writeError(w, http.StatusBadRequest, "invalid request: path "+strconv.Quote(name)+": "+err.Error())
			return
		}
		encoded, err := json.Marshal(fields)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
			return
		}
		call := r.Clone(r.Context())
		call.Body = io.NopCloser(bytes.NewReader(encoded))
		call.ContentLength = int64(len(encoded))
		b.dispatch(w, call, h)
	})
}

// queryField resolves a query parameter's dotted name in desc: every segment but
// the last names a singular message field, the last the field the value fills.
// It answers that field and the object of fields, under fields, that holds it,
// made as it walks.
func queryField(fields map[string]any, desc protoreflect.MessageDescriptor, name string) (map[string]any, protoreflect.FieldDescriptor, error) {
	segments := strings.Split(name, ".")
	at := fields
	for i, seg := range segments {
		fd := rpc.RouteField(desc, seg)
		if fd == nil {
			return nil, nil, fmt.Errorf("%s names no field of %s", seg, desc.FullName())
		}
		if i == len(segments)-1 {
			return at, fd, nil
		}
		if fd.IsList() || fd.IsMap() || fd.Message() == nil {
			return nil, nil, fmt.Errorf("%s is not a singular message field", seg)
		}
		inner, ok := at[fd.JSONName()].(map[string]any)
		if !ok {
			inner = map[string]any{}
			at[fd.JSONName()] = inner
		}
		at, desc = inner, fd.Message()
	}
	return nil, nil, fmt.Errorf("an empty name")
}

// queryValue is a query parameter's values as fd's protojson: an array for a
// repeated field, else its one value.
func queryValue(fd protoreflect.FieldDescriptor, values []string) (json.RawMessage, error) {
	switch {
	case fd.IsMap():
		return nil, fmt.Errorf("%s is a map, which a query parameter cannot set", fd.Name())
	case fd.IsList():
		out := make([]json.RawMessage, 0, len(values))
		for _, s := range values {
			v, err := scalarValue(fd, s)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return json.Marshal(out)
	case len(values) != 1:
		return nil, fmt.Errorf("%s is given %d times and takes one value", fd.Name(), len(values))
	}
	return scalarValue(fd, values[0])
}

// scalarValue is one text value as fd's protojson: a bool as true or false, an
// enum's number as a number and its name as a string, and every other value as
// a JSON string, which protojson parses for numbers, bytes and the well-known
// types and refuses where fd cannot take it.
func scalarValue(fd protoreflect.FieldDescriptor, s string) (json.RawMessage, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("%s takes true or false, got %q", fd.Name(), s)
		}
		return json.Marshal(v)
	case protoreflect.EnumKind:
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			return json.Marshal(n)
		}
	}
	return json.Marshal(s)
}
