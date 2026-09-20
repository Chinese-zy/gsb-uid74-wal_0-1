package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

const testPayloadLen = 32

// payload builds deterministic fixed-width bytes for sequence n (1-based).
func payload(n uint64) []byte {
	b := make([]byte, testPayloadLen)
	binary.BigEndian.PutUint64(b[:8], n)
	for i := 8; i < testPayloadLen; i++ {
		b[i] = byte(i*7 + int(n))
	}
	return b
}

func assertRecords(t *testing.T, got []Record, wantSeqs ...uint64) {
	t.Helper()
	if len(got) != len(wantSeqs) {
		t.Fatalf("record count = %d, want %d (seqs=%v)", len(got), len(wantSeqs), wantSeqs)
	}
	for i, wantSeq := range wantSeqs {
		if got[i].Seq != wantSeq {
			t.Fatalf("record %d: seq = %d, want %d", i, got[i].Seq, wantSeq)
		}
		want := payload(wantSeq)
		if string(got[i].Data) != string(want) {
			t.Fatalf("record %d (seq %d): payload mismatch", i, wantSeq)
		}
	}
}

func TestBasicRoundTrip(t *testing.T) {
	dir := t.TempDir()

	w, recs, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("fresh WAL replayed %d records", len(recs))
	}
	for i := uint64(1); i <= 3; i++ {
		seq, err := w.Append(payload(i))
		if err != nil {
			t.Fatal(err)
		}
		if seq != i {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, recs, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, recs, 1, 2, 3)
	got, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, got, 1, 2, 3)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	const segmentSize = 110 // two frames (2*52=104) fit, the third forces a seal

	w, _, err := Open(dir, WithSegmentSize(segmentSize))
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 6; i++ {
		if _, err := w.Append(payload(i)); err != nil {
			t.Fatal(err)
		}
	}

	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	var sealed, active int
	for _, s := range segs {
		if s.kind == segSealed {
			sealed++
			info, err := os.Stat(s.path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() > segmentSize {
				t.Fatalf("sealed segment %s = %d bytes > %d", s.path, info.Size(), segmentSize)
			}
		} else {
			active++
		}
	}
	if sealed != 2 || active != 1 {
		t.Fatalf("segments: sealed=%d active=%d, want 2 and 1", sealed, active)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	_, recs, err := Open(dir, WithSegmentSize(segmentSize))
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, recs, 1, 2, 3, 4, 5, 6)
}

// flipByte corrupts one byte inside an on-disk segment without touching the
// frame magic, so the scanner must resync after the bad frame.
func flipByte(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := []byte{0}
	if _, err := f.ReadAt(b, offset); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{b[0] ^ 0xff}, offset); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptRecordSkipped(t *testing.T) {
	t.Run("in_active_segment", func(t *testing.T) {
		dir := t.TempDir()
		w, _, err := Open(dir, WithSegmentSize(10_000))
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 5; i++ {
			if _, err := w.Append(payload(i)); err != nil {
				t.Fatal(err)
			}
		}

		// Corrupt a payload byte of record 3 (offset 2*52+20).
		flipByte(t, filepath.Join(dir, segmentName(1, segActive)), 2*(minFrameLen+testPayloadLen)+20)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		_, recs, err := Open(dir, WithSegmentSize(10_000))
		if err != nil {
			t.Fatal(err)
		}
		assertRecords(t, recs, 1, 2, 4, 5)
	})

	t.Run("in_sealed_segment", func(t *testing.T) {
		dir := t.TempDir()
		w, _, err := Open(dir, WithSegmentSize(110)) // 2 frames per segment
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 5; i++ {
			if _, err := w.Append(payload(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Corrupt record 2 inside the first sealed segment.
		flipByte(t, filepath.Join(dir, segmentName(1, segSealed)), minFrameLen+testPayloadLen+20)

		_, recs, err := Open(dir, WithSegmentSize(110))
		if err != nil {
			t.Fatal(err)
		}
		assertRecords(t, recs, 1, 3, 4, 5)

		w2, _, err := Open(dir, WithSegmentSize(110))
		if err != nil {
			t.Fatal(err)
		}
		got, err := w2.ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		assertRecords(t, got, 1, 3, 4, 5)
		if err := w2.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	// Inject a write cut after 10 bytes of the next record: no new frame can
	// validate, so reopening must truncate the tail away.
	w, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(payload(1)); err != nil {
		t.Fatal(err)
	}
	w.failAfter = 10 // arm the cut before the second frame is written
	if _, err := w.Append(payload(2)); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("second append err = %v, want io.ErrShortWrite", err)
	}

	// The WAL refuses more writes after a torn write.
	if _, err := w.Append(payload(3)); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after torn write err = %v, want ErrClosed", err)
	}

	_, recs, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, recs, 1)

	w2, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, segmentName(1, segActive)))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(minFrameLen+testPayloadLen) {
		t.Fatalf("active segment size = %d, want %d", info.Size(), minFrameLen+testPayloadLen)
	}

	// The next append continues cleanly at sequence 2 and keeps the order.
	seq, err := w2.Append(payload(2))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 2 {
		t.Fatalf("seq after repair = %d, want 2", seq)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	_, recs, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, recs, 1, 2)
}

func TestSyncFailure(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, withFailSyncOnce())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(payload(1)); err == nil {
		t.Fatal("append with failing fsync must return an error")
	}

	_, recs, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// The bytes reached the page cache; replay must still expose them in order.
	assertRecords(t, recs, 1)
}

func TestConcurrentAppendsOrderBySeq(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, WithSegmentSize(200)) // force frequent sealing
	if err != nil {
		t.Fatal(err)
	}

	const writers = 8
	const perWriter = 25
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for g := 0; g < writers; g++ {
		wg.Add(1)
		tag := byte(g)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				p := make([]byte, testPayloadLen)
				p[0] = tag
				p[1] = byte(i)
				if _, err := w.Append(p); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	_, recs, err := Open(dir, WithSegmentSize(200))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != writers*perWriter {
		t.Fatalf("replayed %d records, want %d", len(recs), writers*perWriter)
	}
	for i, r := range recs {
		if r.Seq != uint64(i+1) {
			t.Fatalf("visible order broken at %d: seq=%d", i, r.Seq)
		}
	}
}

func TestGarbageBetweenFramesResyncs(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 4; i++ {
		if _, err := w.Append(payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, segmentName(1, segActive))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Insert garbage right before frame 3: frame 3 is then lost, but the
	// scanner must resync on frame 4's magic instead of losing everything
	// after the corruption.
	frame := minFrameLen + testPayloadLen
	garbage := []byte{0x00, 0x01, 'X', 0xde, 0xad, magicPrefix[0]}
	var patched []byte
	patched = append(patched, data[:2*frame]...)
	patched = append(patched, garbage...)
	patched = append(patched, data[2*frame:]...)
	// Actually invalidate frame 3's checksum by flipping a payload byte.
	patched[2*frame+len(garbage)+headerLen] ^= 0xff // corrupt frame 3's payload
	if err := os.WriteFile(path, patched, 0o644); err != nil {
		t.Fatal(err)
	}

	_, recs, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, recs, 1, 2, 4)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(4*frame+len(garbage)) {
		t.Fatalf("active segment after repair = %d bytes, want %d (interior garbage kept, torn tail is the only thing truncated)",
			info.Size(), 4*frame+len(garbage))
	}
}

func TestConcurrentReadersSeeSeqPrefix(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir, WithSegmentSize(200))
	if err != nil {
		t.Fatal(err)
	}

	const writers = 4
	const perWriter = 30
	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(2)
	for r := 0; r < 2; r++ {
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				recs, err := w.ReadAll()
				if err != nil {
					return
				}
				for i, rec := range recs {
					if rec.Seq != uint64(i+1) {
						t.Errorf("reader saw non-prefix order at %d: seq=%d", i, rec.Seq)
						return
					}
				}
			}
		}()
	}

	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := w.Append(payload(uint64(i))); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	readerWG.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionDeletesOldestSealed(t *testing.T) {
	t.Run("cap_holds_a_few_segments", func(t *testing.T) {
		dir := t.TempDir()
		// Frame is 52 bytes; 2 records seal a 110-byte segment (104 bytes).
		w, _, err := Open(dir, WithSegmentSize(110), WithMaxBytes(300))
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 10; i++ {
			if _, err := w.Append(payload(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		segs, err := listSegments(dir)
		if err != nil {
			t.Fatal(err)
		}
		var sealedNames []string
		activeCount := 0
		var total int64
		for _, s := range segs {
			info, err := os.Stat(s.path)
			if err != nil {
				t.Fatal(err)
			}
			total += info.Size()
			if s.kind == segSealed {
				sealedNames = append(sealedNames, filepath.Base(s.path))
			} else {
				activeCount++
			}
		}
		if activeCount != 1 {
			t.Fatalf("active segments = %d, want 1", activeCount)
		}
		if total > 300+104 {
			t.Fatalf("on-disk total %d exceeds cap by more than one segment", total)
		}
		if len(sealedNames) < 1 {
			t.Fatal("all sealed segments were deleted")
		}
		sort.Strings(sealedNames)
		if sealedNames[0] == segmentName(1, segSealed) {
			t.Fatalf("oldest sealed segment still present: %v", sealedNames)
		}

		_, recs, err := Open(dir, WithSegmentSize(110), WithMaxBytes(300))
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			t.Fatal("no records replayable after retention")
		}
		for i := 1; i < len(recs); i++ {
			if recs[i].Seq <= recs[i-1].Seq {
				t.Fatalf("replayed order broken at %d: %d after %d",
					i, recs[i].Seq, recs[i-1].Seq)
			}
		}
		if recs[0].Seq <= 1 {
			t.Fatalf("oldest record seq = %d, expected segment 1 to be deleted", recs[0].Seq)
		}
	})

	t.Run("cap_smaller_than_one_segment", func(t *testing.T) {
		dir := t.TempDir()
		w, _, err := Open(dir, WithSegmentSize(110), WithMaxBytes(10))
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i <= 6; i++ {
			if _, err := w.Append(payload(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		segs, err := listSegments(dir)
		if err != nil {
			t.Fatal(err)
		}
		var activeTmp string
		sealedCount := 0
		for _, s := range segs {
			if s.kind == segActive {
				activeTmp = filepath.Base(s.path)
			} else {
				sealedCount++
			}
		}
		if sealedCount != 1 {
			t.Fatalf("sealed segments = %d, want 1 (newest kept)", sealedCount)
		}
		if activeTmp == "" {
			t.Fatal("active (unsealed) segment must never be deleted")
		}

		_, recs, err := Open(dir, WithSegmentSize(110), WithMaxBytes(10))
		if err != nil {
			t.Fatal(err)
		}
		assertRecords(t, recs, 3, 4, 5, 6)
	})
}

func TestOversizedRecordGetsOwnSegment(t *testing.T) {
	dir := t.TempDir()
	const segmentSize = 60
	w, _, err := Open(dir, WithSegmentSize(segmentSize))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(payload(1)); err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 500)
	for i := range big {
		big[i] = byte(i * 3)
	}
	if _, err := w.Append(big); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(payload(3)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	_, recs, err := Open(dir, WithSegmentSize(segmentSize))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("record count = %d, want 3", len(recs))
	}
	if string(recs[1].Data) != string(big) {
		t.Fatal("oversized payload mismatch after replay")
	}
	assertRecords(t, []Record{recs[0], recs[2]}, 1, 3)
}
