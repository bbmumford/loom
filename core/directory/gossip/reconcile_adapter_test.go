/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"encoding/binary"
	"testing"

	"github.com/bbmumford/whisper"
	"github.com/bbmumford/whisper/iblt"
)

// tableBytes writes a G4 table body in the LAD codec's layout from its header
// fields and cells, so a test can hand the decoder a stream whose header and
// body disagree.
func tableBytes(k uint32, seed uint64, count uint32, mode whisper.ReconcileMode, cells []iblt.Cell) []byte {
	out := make([]byte, 17)
	binary.BigEndian.PutUint32(out[0:4], k)
	binary.BigEndian.PutUint64(out[4:12], seed)
	binary.BigEndian.PutUint32(out[12:16], count)
	out[16] = byte(mode)
	for _, c := range cells {
		out = append(out, c.KeySum[:]...)
		out = binary.BigEndian.AppendUint64(out, c.KeyHashSum)
		out = binary.BigEndian.AppendUint32(out, uint32(c.Count))
	}
	return out
}

// TestATableTheWireDoesNotDescribeIsRefusedBeforeItIsBuilt: the LAD codec
// refuses a table header shorter than its layout, a hash count the IBLT
// would raise or that is implausible, a cell count of zero, and a cell count
// the body does not hold, each before any table is built; a table the bytes
// describe decodes to the shape its header names, cell for cell.
func TestATableTheWireDoesNotDescribeIsRefusedBeforeItIsBuilt(t *testing.T) {
	codec := NewLADReconcileCodec()
	cell := iblt.Cell{KeySum: iblt.Key{1, 2, 3}, KeyHashSum: 0x0102030405060708, Count: 1}
	cases := []struct {
		name string
		body []byte
	}{
		{"header one byte short", tableBytes(3, 9, 1, whisper.ReconcileRate15, []iblt.Cell{cell})[:16]},
		{"k of 0", tableBytes(0, 9, 1, whisper.ReconcileRate15, []iblt.Cell{cell})},
		{"k of 1, which the IBLT would raise to 2", tableBytes(1, 9, 1, whisper.ReconcileRate15, []iblt.Cell{cell})},
		{"k of 17", tableBytes(17, 9, 1, whisper.ReconcileRate15, []iblt.Cell{cell})},
		{"k of 0xFFFFFFFF", tableBytes(0xFFFFFFFF, 9, 1, whisper.ReconcileRate15, []iblt.Cell{cell})},
		{"0 cells, which the IBLT would raise to 1", tableBytes(3, 9, 0, whisper.ReconcileRate15, nil)},
		{"2 cells declared, 1 present", tableBytes(3, 9, 2, whisper.ReconcileRate15, []iblt.Cell{cell})},
		{"0xFFFFFFFF cells declared", tableBytes(3, 9, 0xFFFFFFFF, whisper.ReconcileRate15, []iblt.Cell{cell})},
		{"a cell one byte short", tableBytes(3, 9, 1, whisper.ReconcileRate15, []iblt.Cell{cell})[:17+reconcileCellSize-1]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl, _, err := codec.DecodeTable(tc.body)
			if err == nil || tbl != nil {
				t.Fatalf("decoded a table of %v cells, err %v; want a refusal", tbl != nil, err)
			}
		})
	}

	cells := []iblt.Cell{cell, {KeySum: iblt.Key{9}, KeyHashSum: 7, Count: -1}, {}}
	tbl, mode, err := codec.DecodeTable(tableBytes(4, 0xABCD, 3, whisper.ReconcileRateless, cells))
	if err != nil {
		t.Fatalf("a well-formed table: %v", err)
	}
	if tbl.K() != 4 || tbl.Seed() != 0xABCD || len(tbl.Cells()) != 3 || mode != whisper.ReconcileRateless {
		t.Fatalf("decoded k=%d seed=%#x cells=%d mode=%d", tbl.K(), tbl.Seed(), len(tbl.Cells()), mode)
	}
	for i, c := range tbl.Cells() {
		if c != cells[i] {
			t.Fatalf("cell %d decoded as %+v, want %+v", i, c, cells[i])
		}
	}
	// What the codec writes, it reads back to the same table.
	round, err := codec.EncodeTable(tbl, mode)
	if err != nil {
		t.Fatal(err)
	}
	again, mode2, err := codec.DecodeTable(round)
	if err != nil || mode2 != mode || len(again.Cells()) != 3 || again.K() != 4 || again.Seed() != 0xABCD {
		t.Fatalf("round trip: %v, mode %d, k %d", err, mode2, again.K())
	}
	// The mode byte is carried as written; whisper's responder refuses one
	// outside its two modes before it builds anything.
	_, mode3, err := codec.DecodeTable(tableBytes(3, 9, 1, whisper.ReconcileMode(7), []iblt.Cell{cell}))
	if err != nil || mode3 != 7 {
		t.Fatalf("mode byte 7 decoded as %d, %v", mode3, err)
	}
	if err := whisper.NewReconcileDriver(codec, nil, nil).RunResponderRound(nil, tableBytes(3, 9, 1, whisper.ReconcileMode(7), []iblt.Cell{cell})); err == nil {
		t.Fatalf("the responder built a table for a mode it does not have")
	}
}

// TestReplyAndRequestCountsAreBoundedByTheBody: the reply and request
// decoders refuse a count the body cannot hold before allocating for it, and
// a record whose length runs past the body.
func TestReplyAndRequestCountsAreBoundedByTheBody(t *testing.T) {
	codec := NewLADReconcileCodec()
	reply := func(count uint32, rest ...byte) []byte {
		return append(binary.BigEndian.AppendUint32(nil, count), rest...)
	}
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"short reply", []byte{0, 0}},
		{"count past the body", reply(0xFFFFFFFF)},
		{"count 2, one record", reply(2, 0, 0, 0, 1, 0xAA)},
		{"record length past the body", reply(1, 0, 0, 0, 9, 0xAA)},
		{"record length 0xFFFFFFFF", reply(1, 0xFF, 0xFF, 0xFF, 0xFF, 0xAA)},
	} {
		if out, err := codec.DecodeReply(tc.body); err == nil || out != nil {
			t.Fatalf("%s: decoded %d records, err %v", tc.name, len(out), err)
		}
	}
	out, err := codec.DecodeReply(reply(2, 0, 0, 0, 1, 0xAA, 0, 0, 0, 0))
	if err != nil || len(out) != 2 || out[0][0] != 0xAA || len(out[1]) != 0 {
		t.Fatalf("a well-formed reply: %d records, %v", len(out), err)
	}

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"short request", []byte{0, 0}},
		{"count past the body", reply(0xFFFFFFFF)},
		{"count 2, one id", reply(2, make([]byte, 16)...)},
	} {
		if ids, err := codec.DecodeRequest(tc.body); err == nil || ids != nil {
			t.Fatalf("%s: decoded %d ids, err %v", tc.name, len(ids), err)
		}
	}
	ids, err := codec.DecodeRequest(reply(1, append([]byte{5}, make([]byte, 15)...)...))
	if err != nil || len(ids) != 1 || ids[0][0] != 5 {
		t.Fatalf("a well-formed request: %d ids, %v", len(ids), err)
	}
}
