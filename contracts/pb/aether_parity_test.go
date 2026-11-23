/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package pb

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	aetherpb "github.com/ORBTR/aether/grpc/pb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// schemaLines renders every message, enum and service of a transport schema
// with names relative to its proto package, so two copies that differ only in
// that package render the same lines.
func schemaLines(fd protoreflect.FileDescriptor) []string {
	pkg := string(fd.Package()) + "."
	rel := func(n protoreflect.FullName) string { return strings.TrimPrefix(string(n), pkg) }
	var out []string

	enums := fd.Enums()
	for i := 0; i < enums.Len(); i++ {
		e := enums.Get(i)
		values := e.Values()
		for j := 0; j < values.Len(); j++ {
			v := values.Get(j)
			out = append(out, fmt.Sprintf("enum %s value %s = %d", e.Name(), v.Name(), v.Number()))
		}
	}

	messages := fd.Messages()
	for i := 0; i < messages.Len(); i++ {
		m := messages.Get(i)
		out = append(out, fmt.Sprintf("message %s", m.Name()))
		fields := m.Fields()
		for j := 0; j < fields.Len(); j++ {
			f := fields.Get(j)
			typ := f.Kind().String()
			switch f.Kind() {
			case protoreflect.MessageKind, protoreflect.GroupKind:
				typ = rel(f.Message().FullName())
			case protoreflect.EnumKind:
				typ = rel(f.Enum().FullName())
			}
			out = append(out, fmt.Sprintf("message %s field %d %s %s %s json=%s",
				m.Name(), f.Number(), f.Name(), f.Cardinality(), typ, f.JSONName()))
		}
	}

	services := fd.Services()
	for i := 0; i < services.Len(); i++ {
		s := services.Get(i)
		methods := s.Methods()
		for j := 0; j < methods.Len(); j++ {
			md := methods.Get(j)
			out = append(out, fmt.Sprintf("service %s method %s(%s) %s clientStream=%v serverStream=%v",
				s.Name(), md.Name(), rel(md.Input().FullName()), rel(md.Output().FullName()),
				md.IsStreamingClient(), md.IsStreamingServer()))
		}
	}
	sort.Strings(out)
	return out
}

// mesh_transport.proto here and aether's grpc/pb transport.proto describe one
// transport: aether's grpc package serves and dials it, and this package keeps
// the same schema under the loom.mesh.transport.v1 package name. Every field
// name, number, cardinality and type, every enum value and every method's
// shape must match, so a field renamed or renumbered in one copy fails here
// instead of drifting.
func TestTransportSchemaMatchesAethersCopy(t *testing.T) {
	loom := schemaLines(File_mesh_transport_proto)
	aether := schemaLines(aetherpb.File_pb_transport_proto)

	if len(loom) == 0 {
		t.Fatal("mesh_transport.proto rendered no schema lines")
	}
	inAether := make(map[string]bool, len(aether))
	for _, l := range aether {
		inAether[l] = true
	}
	inLoom := make(map[string]bool, len(loom))
	for _, l := range loom {
		inLoom[l] = true
		if !inAether[l] {
			t.Errorf("only in loom's mesh_transport.proto: %s", l)
		}
	}
	for _, l := range aether {
		if !inLoom[l] {
			t.Errorf("only in aether's transport.proto: %s", l)
		}
	}
}
