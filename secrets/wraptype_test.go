/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package secrets

import (
	"reflect"
	"testing"
)

// RecipientWrap is diststore/crypto's RecipientWrappedDataKey itself, the one
// definition of the wrap's wire type, so a wrap Seal writes is the value
// diststore's unwrap functions and WrappingRecord take, and no second copy of
// the type or its sealing code can drift from it.
func TestRecipientWrapIsDiststoreCryptoType(t *testing.T) {
	typ := reflect.TypeOf(RecipientWrap{})
	if typ.PkgPath() != "github.com/bbmumford/diststore/crypto" || typ.Name() != "RecipientWrappedDataKey" {
		t.Fatalf("RecipientWrap is %s.%s, want github.com/bbmumford/diststore/crypto.RecipientWrappedDataKey", typ.PkgPath(), typ.Name())
	}
}
