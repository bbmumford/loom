package rpc

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// useDeclarationReader makes read the installed reader for the test and puts back
// whatever was installed before it when the test ends. These tests run one at a
// time because the reader is process-wide.
func useDeclarationReader(t *testing.T, read DeclarationReader) {
	t.Helper()
	declarationMu.Lock()
	prev := declarationReader
	declarationReader = read
	declarationMu.Unlock()
	t.Cleanup(func() {
		declarationMu.Lock()
		declarationReader = prev
		declarationMu.Unlock()
	})
}

// declaredHandler is an executable handler under a domain no other test uses, so
// a reader that acts only on it leaves every other registration alone.
func declaredHandler(op string) Handler {
	return Handler{
		Namespace: "orbtr",
		Domain:    "declared",
		Operation: op,
		Func: func(context.Context, proto.Message) (proto.Message, error) {
			return &emptypb.Empty{}, nil
		},
		Request:  (*emptypb.Empty)(nil),
		Response: (*emptypb.Empty)(nil),
		Scope:    ScopePartition,
	}
}

func passThrough(next http.Handler) http.Handler { return next }

func TestTheInstalledReaderWritesTheCopyTheRegistryStores(t *testing.T) {
	useDeclarationReader(t, func(h *Handler) error {
		if h.Domain != "declared" {
			return nil
		}
		h.Exposure = ExposureMeshOnly
		h.Middleware = append(h.Middleware, passThrough)
		return nil
	})

	// The caller's Middleware slice has room past its length, so a reader handed
	// anything but the registry's own copy would append into the caller's array.
	backing := make([]Middleware, 1, 2)
	backing[0] = passThrough
	h := declaredHandler("Write")
	h.Middleware = backing

	reg := NewRegistry()
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, ok := reg.Get(h.FQN())
	if !ok {
		t.Fatalf("%s not stored", h.FQN())
	}
	if !got.MeshOnly() {
		t.Errorf("stored Exposure = %v, want the reader's ExposureMeshOnly", got.Exposure)
	}
	if len(got.Middleware) != 2 {
		t.Errorf("stored Middleware has %d entries, want the caller's one and the reader's one", len(got.Middleware))
	}
	if spare := backing[:cap(backing)][1]; spare != nil {
		t.Error("the reader wrote into the registering caller's Middleware array")
	}
}

func TestRegisterAllStoresNoneOfABatchTheReaderRefuses(t *testing.T) {
	refused := errors.New("the contract refuses B")
	useDeclarationReader(t, func(h *Handler) error {
		if h.Domain == "declared" && h.Operation == "B" {
			return refused
		}
		return nil
	})

	reg := NewRegistry()
	err := reg.RegisterAll([]Handler{declaredHandler("A"), declaredHandler("B")})
	if !errors.Is(err, refused) {
		t.Fatalf("RegisterAll = %v, want the reader's refusal", err)
	}
	if !strings.Contains(err.Error(), "orbtr.declared.B") {
		t.Errorf("refusal %q does not name the refused operation", err)
	}
	if n := reg.Count(); n != 0 {
		t.Errorf("registry holds %d handlers after a refused batch, want 0", n)
	}
}

func TestRegisterProxyIsReadAfterItsFuncAndMiddlewareAreStripped(t *testing.T) {
	var sawFunc, sawMiddleware bool
	useDeclarationReader(t, func(h *Handler) error {
		if h.Domain != "declared" {
			return nil
		}
		sawFunc, sawMiddleware = h.Func != nil, len(h.Middleware) > 0
		h.Middleware = append(h.Middleware, passThrough)
		return nil
	})

	h := declaredHandler("Proxied")
	h.Middleware = []Middleware{passThrough, passThrough}
	reg := NewRegistry()
	if err := reg.RegisterProxy([]Handler{h}); err != nil {
		t.Fatalf("RegisterProxy: %v", err)
	}
	if sawFunc || sawMiddleware {
		t.Errorf("the reader saw Func %v and Middleware %v, want the stripped proxy copy", sawFunc, sawMiddleware)
	}
	got, _ := reg.Get(h.FQN())
	if got.Func != nil || len(got.Middleware) != 1 {
		t.Errorf("stored proxy has Func %v and %d Middleware, want no Func and only the reader's one",
			got.Func != nil, len(got.Middleware))
	}
}

func TestAReaderThatRenamesTheHandlerHasItRefused(t *testing.T) {
	useDeclarationReader(t, func(h *Handler) error {
		if h.Domain == "declared" {
			h.Operation = "Elsewhere"
		}
		return nil
	})

	reg := NewRegistry()
	err := reg.Register(declaredHandler("Named"))
	if err == nil || !strings.Contains(err.Error(), "renamed") {
		t.Fatalf("Register = %v, want the rename refused", err)
	}
	if n := reg.Count(); n != 0 {
		t.Errorf("registry holds %d handlers, want 0", n)
	}
}

func TestInstallDeclarationReaderRefusesNilAndASecondReader(t *testing.T) {
	useDeclarationReader(t, nil)

	if err := InstallDeclarationReader(nil); err == nil {
		t.Error("InstallDeclarationReader(nil) was accepted")
	}
	first := func(*Handler) error { return nil }
	if err := InstallDeclarationReader(first); err != nil {
		t.Fatalf("first install: %v", err)
	}
	if err := InstallDeclarationReader(first); !errors.Is(err, ErrDeclarationReaderInstalled) {
		t.Errorf("second install = %v, want ErrDeclarationReaderInstalled", err)
	}
}

func TestWithNoReaderARegistrationIsStoredAsGiven(t *testing.T) {
	useDeclarationReader(t, nil)

	reg := NewRegistry()
	h := declaredHandler("Plain")
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, _ := reg.Get(h.FQN())
	if got.MeshOnly() || len(got.Middleware) != 0 {
		t.Errorf("stored handler changed with no reader installed: Exposure %v, %d Middleware", got.Exposure, len(got.Middleware))
	}
}
