// Package wal implements a segment based write-ahead log.
//
// Each record is appended to the active segment. When a record does not fit
// in the active segment it is sealed and a new segment is opened. Records are
// framed with a checksum so a torn tail or a corrupted record in the middle of
// a segment can be skipped without disturbing the sequence numbers of the
// records that follow.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

const (
	// magicPrefix marks the start of a record frame.
	magicPrefix = "WALR"
	headerLen   = 4 + 8 + 4 // magic + seq + payload length
	crcLen      = 4
	minFrameLen = headerLen + crcLen

	sealedSuffix = ".wal"
	activeSuffix = ".tmp"

	// defaultSegmentSize is the target size of a sealed segment.
	defaultSegmentSize = 64 * 1024 * 1024

	// minSegmentSize leaves room for at least one small frame per segment.
	minSegmentSize = minFrameLen + 1
)

var (
	// ErrClosed is returned when the WAL has been closed or marked broken.
	ErrClosed = errors.New("wal: closed")
	// ErrPayloadTooLarge is returned for payloads whose frame cannot be encoded.
	ErrPayloadTooLarge = errors.New("wal: payload too large")
	// ErrInvalidOption is returned for inconsistent options.
	ErrInvalidOption = errors.New("wal: invalid option")
)

// Record is one decoded log entry.
type Record struct {
	Seq  uint64
	Data []byte
}

// Option configures a WAL.
type Option func(*config)

type config struct {
	segmentSize int
	maxBytes    int64 // 0 means unlimited

	// Test-only fault injection points.
	failAfter int64 // inject a write error after this many bytes; <0 disables
	syncFunc  func(*os.File) error
	failSync  bool // inject exactly one fsync failure
}

// WithSegmentSize sets the target size of a sealed segment. A record larger
// than the segment is still accepted: it gets a segment of its own.
func WithSegmentSize(bytes int) Option {
	return func(c *config) { c.segmentSize = bytes }
}

// WithMaxBytes caps the total bytes kept on disk. After a segment is sealed
// the oldest sealed segments are deleted until the limit is met. The active
// (unsealed) segment is never deleted.
func WithMaxBytes(bytes int64) Option {
	return func(c *config) { c.maxBytes = bytes }
}

// withFailAfter injects a write failure after n bytes have been written.
func withFailAfter(n int64) Option {
	return func(c *config) { c.failAfter = n }
}

// withSyncFunc replaces file syncing, allowing fsync failures in tests.
func withSyncFunc(f func(*os.File) error) Option {
	return func(c *config) { c.syncFunc = f }
}

// withFailSyncOnce injects a single failing fsync.
func withFailSyncOnce() Option {
	return func(c *config) { c.failSync = true }
}

// WAL is a write-ahead log backed by numbered segment files.
type WAL struct {
	mu sync.RWMutex

	dir       string
	cfg       config
	active    *os.File
	activeN   int64  // bytes used in the active segment
	nextIdx   uint64 // index used for the next new segment
	nextSeq   uint64 // sequence number of the next record
	failAfter int64  // one-shot: cut a write off after this many bytes
	failSync  bool   // one-shot: make the next fsync fail
	closed    bool
}

type segmentKind int

const (
	segSealed segmentKind = iota
	segActive
)

type segment struct {
	idx  uint64
	kind segmentKind
	path string
}

// Open opens (or creates) the WAL in dir and replays every surviving record
// in original order. A torn tail in the active segment is truncated; corrupted
// records in the middle of a segment are skipped.
func Open(dir string, opts ...Option) (*WAL, []Record, error) {
	cfg := config{
		segmentSize: defaultSegmentSize,
		maxBytes:    0,
		failAfter:   -1,
		syncFunc:    func(f *os.File) error { return f.Sync() },
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.segmentSize < minSegmentSize {
		return nil, nil, fmt.Errorf("%w: segment size %d below minimum %d",
			ErrInvalidOption, cfg.segmentSize, minSegmentSize)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}

	segs, err := listSegments(dir)
	if err != nil {
		return nil, nil, err
	}

	w := &WAL{dir: dir, cfg: cfg, nextSeq: 1}
	w.failAfter = cfg.failAfter
	w.failSync = cfg.failSync

	var replayed []Record
	var activeSeg *segment
	var validEnd int64

	for i := range segs {
		seg := segs[i]
		data, err := os.ReadFile(seg.path)
		if err != nil {
			return nil, nil, err
		}
		recs, end := scanFrames(data)
		for _, r := range recs {
			if r.Seq < w.nextSeq {
				continue // stale duplicate; gaps stay as gaps
			}
			w.nextSeq = r.Seq + 1
			replayed = append(replayed, r)
		}
		if seg.kind == segActive {
			activeSeg = &segs[i]
			validEnd = end
		}
	}

	if activeSeg != nil {
		info, err := os.Stat(activeSeg.path)
		if err != nil {
			return nil, nil, err
		}
		if info.Size() != validEnd {
			// Drop the torn tail: everything after the last valid frame.
			f, err := os.OpenFile(activeSeg.path, os.O_RDWR, 0o644)
			if err != nil {
				return nil, nil, err
			}
			if err := f.Truncate(validEnd); err != nil {
				f.Close()
				return nil, nil, err
			}
			if err := cfg.syncFunc(f); err != nil {
				f.Close()
				return nil, nil, err
			}
			if err := f.Close(); err != nil {
				return nil, nil, err
			}
		}
		w.activeN = validEnd
		w.nextIdx = activeSeg.idx
		f, err := os.OpenFile(activeSeg.path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, nil, err
		}
		if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
			f.Close()
			return nil, nil, err
		}
		w.active = f
	} else {
		idx := uint64(1)
		if len(segs) > 0 {
			idx = segs[len(segs)-1].idx + 1
		}
		w.nextIdx = idx
		if err := w.createActive(); err != nil {
			return nil, nil, err
		}
	}

	return w, replayed, nil
}

// Append writes one record, fsyncs it, and returns its sequence number.
// Sequence numbers start at 1 and are contiguous for records visible after a
// clean close; gaps mark records that were lost or corrupted on disk.
func (w *WAL) Append(payload []byte) (uint64, error) {
	if uint64(len(payload)) > 0xffffffff {
		return 0, ErrPayloadTooLarge
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}

	frame := encodeFrame(w.nextSeq, payload)

	// Rotate when the frame does not fit and the active segment is non-empty,
	// so a sealed segment never starts with a record spilled from its
	// predecessor and an oversized record gets a segment of its own.
	if w.activeN > 0 && int64(len(frame)) > int64(w.cfg.segmentSize)-w.activeN {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}

	if err := writeFull(w.active, frame, w.consumeFailAfter()); err != nil {
		w.markBrokenLocked()
		return 0, err
	}
	if err := w.syncLocked(w.active); err != nil {
		w.markBrokenLocked()
		return 0, err
	}

	seq := w.nextSeq
	w.nextSeq++
	w.activeN += int64(len(frame))
	return seq, nil
}

// ReadAll returns every surviving record in segment order. Corrupted records
// are skipped; the surviving records keep their original sequence numbers.
func (w *WAL) ReadAll() ([]Record, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return nil, ErrClosed
	}

	segs, err := listSegments(w.dir)
	if err != nil {
		return nil, err
	}

	var out []Record
	var lastSeq uint64
	for _, seg := range segs {
		data, err := os.ReadFile(seg.path)
		if err != nil {
			return nil, err
		}
		recs, _ := scanFrames(data)
		for _, r := range recs {
			if r.Seq <= lastSeq {
				continue
			}
			lastSeq = r.Seq
			out = append(out, r)
		}
	}
	return out, nil
}

// Close fsyncs and closes the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.active == nil {
		return nil
	}
	err := w.syncLocked(w.active)
	if cerr := w.active.Close(); err == nil {
		err = cerr
	}
	w.active = nil
	return err
}

// markBrokenLocked refuses further writes after a torn/failed write because the
// active segment is in an unknown state until reopen repairs it.
func (w *WAL) markBrokenLocked() {
	w.closed = true
	if w.active != nil {
		w.active.Close()
		w.active = nil
	}
}

// consumeFailAfter returns the one-shot torn-write injection point.
func (w *WAL) consumeFailAfter() int64 {
	n := w.failAfter
	w.failAfter = -1
	return n
}

// syncLocked fsyncs f, failing exactly once when the test hook is armed.
func (w *WAL) syncLocked(f *os.File) error {
	if w.failSync {
		w.failSync = false
		return errors.New("wal: injected fsync failure")
	}
	return w.cfg.syncFunc(f)
}

// createActive opens a new active tmp segment using nextIdx.
func (w *WAL) createActive() error {
	path := filepath.Join(w.dir, segmentName(w.nextIdx, segActive))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.active = f
	w.activeN = 0
	return nil
}

// rotateLocked seals the active tmp segment as .wal and opens a fresh one.
func (w *WAL) rotateLocked() error {
	if err := w.syncLocked(w.active); err != nil {
		w.markBrokenLocked()
		return err
	}
	if err := w.active.Close(); err != nil {
		w.markBrokenLocked()
		return err
	}
	oldTmp := filepath.Join(w.dir, segmentName(w.nextIdx, segActive))
	oldWal := filepath.Join(w.dir, segmentName(w.nextIdx, segSealed))
	if err := os.Rename(oldTmp, oldWal); err != nil {
		w.markBrokenLocked()
		return err
	}
	if err := syncDir(w.dir); err != nil {
		w.markBrokenLocked()
		return err
	}

	w.nextIdx++
	if err := w.createActive(); err != nil {
		return err
	}
	if w.cfg.maxBytes > 0 {
		if err := w.enforceRetentionLocked(); err != nil {
			return err
		}
	}
	return nil
}

// enforceRetentionLocked deletes the oldest sealed segments while total on-disk
// usage exceeds the cap. The active tmp segment is never eligible, and the
// newest sealed segment is kept even if the cap is tiny.
func (w *WAL) enforceRetentionLocked() error {
	for {
		entries, err := os.ReadDir(w.dir)
		if err != nil {
			return err
		}
		type fileInfo struct {
			name string
			idx  uint64
		}
		var sealed []fileInfo
		var total int64
		for _, e := range entries {
			idx, kind, ok := parseSegmentName(e.Name())
			if !ok {
				continue
			}
			info, err := e.Info()
			if err != nil {
				return err
			}
			total += info.Size()
			if kind == segSealed {
				sealed = append(sealed, fileInfo{name: e.Name(), idx: idx})
			}
		}
		if total <= w.cfg.maxBytes || len(sealed) <= 1 {
			return nil
		}
		sort.Slice(sealed, func(i, j int) bool { return sealed[i].idx < sealed[j].idx })
		if err := os.Remove(filepath.Join(w.dir, sealed[0].name)); err != nil {
			return err
		}
		if err := syncDir(w.dir); err != nil {
			return err
		}
	}
}

// encodeFrame builds magic | seq | len | payload | crc32(seq|len|payload).
func encodeFrame(seq uint64, payload []byte) []byte {
	buf := make([]byte, headerLen+len(payload)+crcLen)
	copy(buf[0:4], magicPrefix)
	binary.BigEndian.PutUint64(buf[4:12], seq)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(payload)))
	copy(buf[headerLen:], payload)
	sum := crc32.ChecksumIEEE(buf[4 : headerLen+len(payload)])
	binary.BigEndian.PutUint32(buf[headerLen+len(payload):], sum)
	return buf
}

// scanFrames extracts every valid frame from data in order and returns the byte
// offset immediately after the last valid frame. Invalid bytes (a corrupted
// frame, a torn tail) are skipped by sliding forward one byte and resyncing on
// the next magic, so a bad record never shifts the sequence of later records.
func scanFrames(data []byte) ([]Record, int64) {
	var recs []Record
	off := 0
	validEnd := 0
	for off+minFrameLen <= len(data) {
		if string(data[off:off+4]) != magicPrefix {
			off++
			continue
		}
		seq := binary.BigEndian.Uint64(data[off+4 : off+12])
		payloadLen := int(binary.BigEndian.Uint32(data[off+12 : off+16]))
		if payloadLen > len(data) {
			off++
			continue
		}
		end := off + minFrameLen + payloadLen
		if end > len(data) {
			off++ // torn tail; keep scanning is pointless but stay defensive
			continue
		}
		wantSum := binary.BigEndian.Uint32(data[end-crcLen : end])
		gotSum := crc32.ChecksumIEEE(data[off+4 : end-crcLen])
		if wantSum != gotSum {
			off++ // corrupted record: skip it and resync after it
			continue
		}
		payload := make([]byte, payloadLen)
		copy(payload, data[off+headerLen:end-crcLen])
		recs = append(recs, Record{Seq: seq, Data: payload})
		validEnd = end
		off = end
	}
	return recs, int64(validEnd)
}

// writeFull writes all of p to w. failAfter (when >= 0) simulates a write that
// is cut off partway through, leaving a torn tail behind.
func writeFull(w io.Writer, p []byte, failAfter int64) error {
	if failAfter >= 0 {
		if failAfter < int64(len(p)) {
			if failAfter > 0 {
				if _, err := w.Write(p[:failAfter]); err != nil {
					return err
				}
			}
			return io.ErrShortWrite
		}
	}
	_, err := w.Write(p)
	return err
}

// segmentName formats a segment file name.
func segmentName(idx uint64, kind segmentKind) string {
	suffix := sealedSuffix
	if kind == segActive {
		suffix = activeSuffix
	}
	return fmt.Sprintf("%020d%s", idx, suffix)
}

// parseSegmentName parses names produced by segmentName.
func parseSegmentName(name string) (uint64, segmentKind, bool) {
	var kind segmentKind
	switch {
	case len(name) > len(sealedSuffix) && name[len(name)-len(sealedSuffix):] == sealedSuffix:
		kind = segSealed
	case len(name) > len(activeSuffix) && name[len(name)-len(activeSuffix):] == activeSuffix:
		kind = segActive
	default:
		return 0, 0, false
	}
	stem := name[:len(name)-len(sealedSuffix)]
	idx, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return idx, kind, true
}

// listSegments returns all segment files sorted by index, sealed before the
// active tmp of the same index.
func listSegments(dir string) ([]segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []segment
	for _, e := range entries {
		idx, kind, ok := parseSegmentName(e.Name())
		if !ok {
			continue
		}
		segs = append(segs, segment{
			idx:  idx,
			kind: kind,
			path: filepath.Join(dir, e.Name()),
		})
	}
	sort.Slice(segs, func(i, j int) bool {
		if segs[i].idx != segs[j].idx {
			return segs[i].idx < segs[j].idx
		}
		return segs[i].kind < segs[j].kind
	})
	return segs, nil
}

// syncDir fsyncs a directory so renames and unlinks survive a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	if closeErr := d.Close(); syncErr == nil {
		syncErr = closeErr
	}
	return syncErr
}
