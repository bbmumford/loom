/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package journal provides the standard ports.DurableJournal: a
// crash-consistent, append-only file journal of accepted records.
//
// This is the Phase-0.5 "narrow journal port" implementation (plan §0.5.1):
// it records canonical, already-authorized records for recovery and audit.
// It is NEVER a merge authority — replay feeds records back through the
// same projection path that accepted them live.
//
// Format: one file of length-prefixed, CRC-guarded JSON entries
//
//	[4B big-endian length][4B big-endian CRC32(payload)][payload JSON]
//
// where payload = {"w": watermark, "r": ports.Record}. JSON base64-encodes
// every byte field, so signed fields round-trip byte-identically. On open,
// the file is scanned to the last valid entry; a torn tail (crash mid-
// append) is truncated — everything before it is intact by construction.
package journal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bbmumford/loom/ports"
)

// ErrClosed is returned by operations on a closed journal.
var ErrClosed = errors.New("journal: closed")

type entry struct {
	W ports.Watermark `json:"w"`
	R ports.Record    `json:"r"`
}

// FileJournal implements ports.DurableJournal over a single append-only
// file. All appends are serialized and fsynced before their watermark is
// exposed — Append returning means the record survives a crash.
type FileJournal struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	head   ports.Watermark // highest DURABLE (fsynced) watermark
	next   ports.Watermark // highest staged watermark (== head outside a batch)
	closed bool

	subSeq uint64
	subs   map[uint64]*replaySub
}

type replaySub struct {
	topics map[ports.Topic]bool // nil = all
	from   ports.Watermark      // live records ≤ from are filtered
	ch     chan ports.Record
}

// Open opens (or creates) the journal at dir/loom-journal.log, recovering
// the head watermark by scanning to the last valid entry and truncating any
// torn tail.
func Open(dir string) (*FileJournal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("journal: mkdir: %w", err)
	}
	path := filepath.Join(dir, "loom-journal.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("journal: open: %w", err)
	}
	validEnd, head, err := scanLastValid(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(validEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("journal: truncate torn tail: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &FileJournal{
		f:    f,
		path: path,
		head: head,
		next: head,
		subs: make(map[uint64]*replaySub),
	}, nil
}

// scanLastValid walks the file entry-by-entry, returning the byte offset
// after the last valid entry and its watermark.
func scanLastValid(f *os.File) (int64, ports.Watermark, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}
	var (
		off  int64
		head ports.Watermark
		hdr  [8]byte
	)
	for {
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			break // clean EOF or torn header — stop here
		}
		length := binary.BigEndian.Uint32(hdr[:4])
		want := binary.BigEndian.Uint32(hdr[4:])
		if length == 0 || length > 64<<20 {
			break // corrupt length — truncate here
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			break // torn payload
		}
		if crc32.ChecksumIEEE(payload) != want {
			break // corrupt payload
		}
		var e entry
		if err := json.Unmarshal(payload, &e); err != nil {
			break
		}
		off += int64(8 + length)
		head = e.W
	}
	return off, head, nil
}

// Append implements ports.DurableJournal.
func (j *FileJournal) Append(_ context.Context, rec ports.Record) (ports.Watermark, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return 0, ErrClosed
	}
	startOff, err := j.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	w, err := j.stageLocked(rec)
	if err != nil {
		j.rollbackLocked(startOff)
		return 0, err
	}
	if err := j.f.Sync(); err != nil {
		j.rollbackLocked(startOff)
		return 0, fmt.Errorf("journal: fsync: %w", err)
	}
	j.head = j.next
	j.fanoutLocked(w, rec)
	return w, nil
}

// BatchAppend implements ports.DurableJournal — all-or-nothing: entries are
// staged then fsynced ONCE; any failure truncates back to the pre-batch
// offset so no partial batch is ever exposed or recovered.
func (j *FileJournal) BatchAppend(_ context.Context, recs []ports.Record) (ports.Watermark, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return 0, ErrClosed
	}
	if len(recs) == 0 {
		return j.head, nil
	}
	startOff, err := j.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	staged := make([]ports.Watermark, 0, len(recs))
	for _, rec := range recs {
		w, err := j.stageLocked(rec)
		if err != nil {
			j.rollbackLocked(startOff)
			return 0, err
		}
		staged = append(staged, w)
	}
	if err := j.f.Sync(); err != nil {
		j.rollbackLocked(startOff)
		return 0, fmt.Errorf("journal: fsync: %w", err)
	}
	j.head = j.next
	for i, rec := range recs {
		j.fanoutLocked(staged[i], rec)
	}
	return j.head, nil
}

// stageLocked writes one entry (no fsync; head unchanged). Caller holds mu.
func (j *FileJournal) stageLocked(rec ports.Record) (ports.Watermark, error) {
	w := j.next + 1
	payload, err := json.Marshal(entry{W: w, R: rec})
	if err != nil {
		return 0, fmt.Errorf("journal: encode: %w", err)
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
	if _, err := j.f.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := j.f.Write(payload); err != nil {
		return 0, err
	}
	j.next = w
	return w, nil
}

// rollbackLocked undoes staged-but-not-durable writes. Caller holds mu.
func (j *FileJournal) rollbackLocked(startOff int64) {
	_ = j.f.Truncate(startOff)
	_, _ = j.f.Seek(startOff, io.SeekStart)
	j.next = j.head
}

// Head implements ports.DurableJournal.
func (j *FileJournal) Head(_ context.Context) (ports.Watermark, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return 0, ErrClosed
	}
	return j.head, nil
}

// Replay implements ports.DurableJournal: history from the file, then live
// appends, gap-free. The live sink is registered under the append lock
// BEFORE history is read, so no record can fall between history's end and
// live's start; the sink filters w ≤ max(from, historyEnd) so nothing is
// duplicated either.
func (j *FileJournal) Replay(ctx context.Context, from ports.Watermark, topics []ports.Topic) (<-chan ports.Record, error) {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return nil, ErrClosed
	}
	var topicSet map[ports.Topic]bool
	if len(topics) > 0 {
		topicSet = make(map[ports.Topic]bool, len(topics))
		for _, t := range topics {
			topicSet[t] = true
		}
	}
	historyEnd := j.head
	liveFloor := historyEnd
	if from > liveFloor {
		liveFloor = from
	}
	sub := &replaySub{topics: topicSet, from: liveFloor, ch: make(chan ports.Record, 256)}
	id := j.subSeq
	j.subSeq++
	j.subs[id] = sub
	readF, err := os.Open(j.path)
	j.mu.Unlock()
	if err != nil {
		j.dropSub(id)
		return nil, err
	}

	out := make(chan ports.Record, 256)
	go func() {
		defer close(out)
		defer j.dropSub(id)
		defer readF.Close()
		// 1. History: everything in (from, historyEnd].
		var hdr [8]byte
		for {
			if _, err := io.ReadFull(readF, hdr[:]); err != nil {
				break
			}
			length := binary.BigEndian.Uint32(hdr[:4])
			payload := make([]byte, length)
			if _, err := io.ReadFull(readF, payload); err != nil {
				break
			}
			var e entry
			if json.Unmarshal(payload, &e) != nil {
				break
			}
			if e.W > historyEnd {
				break // the live channel owns everything past the snapshot
			}
			if e.W <= from || (topicSet != nil && !topicSet[e.R.Topic]) {
				continue
			}
			select {
			case out <- e.R:
			case <-ctx.Done():
				return
			}
		}
		// 2. Live tail.
		for {
			select {
			case r, ok := <-sub.ch:
				if !ok {
					return
				}
				select {
				case out <- r:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// ReplayUntil implements ports.DurableJournal: history-only replay of
// (from, to], closing the channel at the end — the boot-time projection
// rebuild path.
func (j *FileJournal) ReplayUntil(ctx context.Context, from, to ports.Watermark, topics []ports.Topic) (<-chan ports.Record, error) {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return nil, ErrClosed
	}
	var topicSet map[ports.Topic]bool
	if len(topics) > 0 {
		topicSet = make(map[ports.Topic]bool, len(topics))
		for _, t := range topics {
			topicSet[t] = true
		}
	}
	readF, err := os.Open(j.path)
	j.mu.Unlock()
	if err != nil {
		return nil, err
	}

	out := make(chan ports.Record, 256)
	go func() {
		defer close(out)
		defer readF.Close()
		var hdr [8]byte
		for {
			if _, err := io.ReadFull(readF, hdr[:]); err != nil {
				return
			}
			length := binary.BigEndian.Uint32(hdr[:4])
			payload := make([]byte, length)
			if _, err := io.ReadFull(readF, payload); err != nil {
				return
			}
			var e entry
			if json.Unmarshal(payload, &e) != nil {
				return
			}
			if e.W > to {
				return
			}
			if e.W <= from || (topicSet != nil && !topicSet[e.R.Topic]) {
				continue
			}
			select {
			case out <- e.R:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (j *FileJournal) dropSub(id uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.subs, id)
}

// fanoutLocked pushes a newly-durable record to live subscribers. Caller
// holds mu. A full subscriber buffer does NOT block the append path — the
// record is already durable, so a slow consumer recovers by resuming a
// fresh Replay from its last processed watermark (§0.5.4: resume from a
// watermark, no silent gap — the file is the source of truth, the live
// channel is only a latency optimisation).
func (j *FileJournal) fanoutLocked(w ports.Watermark, rec ports.Record) {
	for _, sub := range j.subs {
		if w <= sub.from {
			continue
		}
		if sub.topics != nil && !sub.topics[rec.Topic] {
			continue
		}
		select {
		case sub.ch <- rec:
		default:
		}
	}
}

// Snapshot implements ports.DurableJournal: a consistent point-in-time
// stream of the journal file (everything durable at call time).
func (j *FileJournal) Snapshot(_ context.Context) (io.ReadCloser, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, ErrClosed
	}
	end, err := j.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	readF, err := os.Open(j.path)
	if err != nil {
		return nil, err
	}
	return &boundedReadCloser{f: readF, remaining: end}, nil
}

type boundedReadCloser struct {
	f         *os.File
	remaining int64
}

func (b *boundedReadCloser) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.f.Read(p)
	b.remaining -= int64(n)
	return n, err
}

func (b *boundedReadCloser) Close() error { return b.f.Close() }

// Compact implements ports.DurableJournal: rewrites the journal retaining
// (a) every entry with watermark > upTo, and (b) for entries ≤ upTo, the
// LATEST entry per (topic,node) slot — so no slot's current state and no
// governing tombstone is ever dropped by retention (§0.5.4: tombstone GC by
// acknowledged checkpoint, never wall clock). Atomic: temp + fsync + rename.
func (j *FileJournal) Compact(_ context.Context, upTo ports.Watermark) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}

	// Pass 1: latest watermark per slot within the ≤ upTo region.
	latest := map[string]ports.Watermark{}
	if err := j.scanLocked(func(e entry) {
		if e.W <= upTo {
			latest[slotKey(e.R)] = e.W
		}
	}); err != nil {
		return err
	}

	// Pass 2: rewrite survivors to a temp file.
	tmpPath := j.path + ".compact"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	writeErr := error(nil)
	if err := j.scanLocked(func(e entry) {
		if writeErr != nil {
			return
		}
		if !(e.W > upTo || latest[slotKey(e.R)] == e.W) {
			return
		}
		payload, err := json.Marshal(e)
		if err != nil {
			writeErr = err
			return
		}
		var hdr [8]byte
		binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
		if _, err := tmp.Write(hdr[:]); err != nil {
			writeErr = err
			return
		}
		if _, err := tmp.Write(payload); err != nil {
			writeErr = err
		}
	}); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if writeErr == nil {
		writeErr = tmp.Sync()
	}
	tmp.Close()
	if writeErr != nil {
		os.Remove(tmpPath)
		return writeErr
	}

	// Swap atomically. Close the live handle first (Windows rename
	// semantics), reopen at the end.
	if err := j.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		j.f, _ = os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0o644)
		return fmt.Errorf("journal: compact rename: %w", err)
	}
	f, err := os.OpenFile(j.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	j.f = f
	return nil
}

// scanLocked iterates every valid entry in the current file. Caller holds mu.
func (j *FileJournal) scanLocked(fn func(entry)) error {
	readF, err := os.Open(j.path)
	if err != nil {
		return err
	}
	defer readF.Close()
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(readF, hdr[:]); err != nil {
			return nil
		}
		length := binary.BigEndian.Uint32(hdr[:4])
		payload := make([]byte, length)
		if _, err := io.ReadFull(readF, payload); err != nil {
			return nil
		}
		var e entry
		if json.Unmarshal(payload, &e) != nil {
			return nil
		}
		fn(e)
	}
}

// Close implements ports.DurableJournal.
func (j *FileJournal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	for _, sub := range j.subs {
		close(sub.ch)
	}
	j.subs = map[uint64]*replaySub{}
	return j.f.Close()
}

func slotKey(r ports.Record) string {
	return string(r.Topic) + "\x00" + string(r.NodeID)
}

var _ ports.DurableJournal = (*FileJournal)(nil)
