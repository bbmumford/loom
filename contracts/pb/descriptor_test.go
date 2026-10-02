package pb

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/types/descriptorpb"
)

// wantProtoPackage is the proto package mesh_transport.proto declares. gRPC
// sends it on the wire as the first segment of every method path.
const wantProtoPackage = "loom.mesh.transport.v1"

// TestDescriptorPackage asserts the registered descriptor declares the neutral
// proto package, that its go_package is the import path this generated code
// compiles under, and that the service name and method paths gRPC routes by
// are qualified by that package.
func TestDescriptorPackage(t *testing.T) {
	fd := File_mesh_transport_proto
	if got := string(fd.Package()); got != wantProtoPackage {
		t.Fatalf("proto package = %q, want %q", got, wantProtoPackage)
	}
	opts, ok := fd.Options().(*descriptorpb.FileOptions)
	if !ok {
		t.Fatalf("file options have type %T, want *descriptorpb.FileOptions", fd.Options())
	}
	want := reflect.TypeOf((*Frame)(nil)).Elem().PkgPath()
	if got := opts.GetGoPackage(); got != want {
		t.Fatalf("go_package = %q, want the compiled import path %q", got, want)
	}

	const service = wantProtoPackage + ".TransportService"
	if got := TransportService_ServiceDesc.ServiceName; got != service {
		t.Fatalf("service name = %q, want %q", got, service)
	}
	for got, want := range map[string]string{
		TransportService_Stream_FullMethodName: "/" + service + "/Stream",
		TransportService_Ping_FullMethodName:   "/" + service + "/Ping",
	} {
		if got != want {
			t.Fatalf("method path = %q, want %q", got, want)
		}
	}
}
