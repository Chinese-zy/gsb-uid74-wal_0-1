package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// fixedPayload 生成内容完全确定的记录：序号以小端 8 字节开头，
// 后面用确定的字节模式填满 n 个字节，方便逐字节比对。
func fixedPayload(seq uint64, n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte((int(seq)*31 + i) % 251)
	}
	p[0] = byte(seq)
	p[1] = byte(seq >> 8)
	p[2] = byte(seq >> 16)
	p[3] = byte(seq >> 24)
	p[4] = byte(seq >> 32)
	p[5] = byte(seq >> 40)
	p[6] = byte(seq >> 48)
	p[7] = byte(seq >> 56)
	return p
}

func payloadSeq(p []byte) uint64 {
	var seq uint64
	for i := 0; i < 8; i++ {
		seq |= uint64(p[i]) << (8 * i)
	}
	return seq
}

func readAll(t *testing.T, w *WAL) [][]byte {
	t.Helper()
	r, err := w.NewReader()
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	var got [][]byte
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, rec)
	}
	return got
}

// reopen 模拟重新打开进程：同一目录再 Open 一次。
func reopen(t *testing.T, dir string, opt Options) *WAL {
	t.Helper()
	w, err := Open(dir, opt)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return w
}

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	opt := Options{SegmentBytes: 64, MaxBytes: 1 << 20}
	w := reopen(t, dir, opt)

	const n = 40
	for i := 1; i <= n; i++ {
		seq, err := w.Append(fixedPayload(uint64(i), 10))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != uint64(i) {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w = reopen(t, dir, opt)
	got := readAll(t, w)
	if len(got) != n {
		t.Fatalf("records = %d, want %d", len(got), n)
	}
	for i, rec := range got {
		want := fixedPayload(uint64(i+1), 10)
		if payloadSeq(rec) != uint64(i+1) || !bytes.Equal(rec, want) {
			t.Fatalf("record %d mismatch", i+1)
		}
	}
}

// TestCorruptRecordSkipped：中间一条的校验和坏了必须跳过，
// 后面好记录的序号和内容不能错位。
func TestCorruptRecordSkipped(t *testing.T) {
	dir := t.TempDir()
	opt := Options{SegmentBytes: 1 << 20, MaxBytes: 1 << 20}
	w := reopen(t, dir, opt)
	for i := 1; i <= 6; i++ {
		if _, err := w.Append(fixedPayload(uint64(i), 16)); err != nil {
			t.Fatal(err)
		}
	}

	// 直接在段文件里把第 4 条的一个载荷字节翻转（保持长度不变，只让 CRC 失败）。
	path := filepath.Join(dir, segName(1))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	off := int64(0)
	for frame := 1; frame < 4; frame++ {
		off += int64(frameHeader) + 16
	}
	raw[off+frameHeader] ^= 0xFF
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	w = reopen(t, dir, opt)
	got := readAll(t, w)
	wantSeq := []uint64{1, 2, 3, 5, 6}
	if len(got) != len(wantSeq) {
		t.Fatalf("records = %d, want %d (%v)", len(got), len(wantSeq), got)
	}
	for i, rec := range got {
		if payloadSeq(rec) != wantSeq[i] {
			t.Fatalf("record %d has seq %d, want %d（序号错位）", i, payloadSeq(rec), wantSeq[i])
		}
	}
	// 新追加继续从 7 开始，不与坏记录冲突。
	seq, err := w.Append(fixedPayload(7, 16))
	if err != nil || seq != 7 {
		t.Fatalf("append after corrupt: seq=%d err=%v", seq, err)
	}
}

// TestTornWriteRecovery：注入“写到一半被掐掉”，重新打开后
// 半截尾巴丢掉，已落盘的记录按原顺序吐出，序号接着用。
func TestTornWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	opt := Options{
		SegmentBytes: 1 << 20,
		MaxBytes:     1 << 20,
		Fault: FaultHooks{
			BeforeFrameWrite: func(seq uint64) (int, error) {
				if seq == 4 {
					return 7, errors.New("boom: power lost mid-frame") // 只落盘 7 个字节
				}
				return 0, nil
			},
		},
	}
	w := reopen(t, dir, opt)
	for i := 1; i <= 3; i++ {
		if _, err := w.Append(fixedPayload(uint64(i), 12)); err != nil {
			t.Fatal(err)
		}
	}
	_, err := w.Append(fixedPayload(4, 12))
	if !errors.Is(err, ErrRecoverNeeded) {
		t.Fatalf("want ErrRecoverNeeded, got %v", err)
	}

	before, _ := os.ReadFile(filepath.Join(dir, segName(1)))

	// 模拟重新打开进程（故障点取消）。
	clean := Options{SegmentBytes: 1 << 20, MaxBytes: 1 << 20}
	w2 := reopen(t, dir, clean)
	after, _ := os.ReadFile(filepath.Join(dir, segName(1)))
	goodTail := int64(3 * (frameHeader + 12))
	if int64(len(after)) != goodTail {
		t.Fatalf("半截尾巴没丢：文件 %d 字节，应截断到 %d", len(after), goodTail)
	}
	if int64(len(before)) == goodTail {
		t.Fatal("注入没有真的留下半截字节")
	}

	got := readAll(t, w2)
	if len(got) != 3 {
		t.Fatalf("replay records = %d, want 3", len(got))
	}
	for i, rec := range got {
		if payloadSeq(rec) != uint64(i+1) {
			t.Fatalf("已落盘记录顺序错：第 %d 条 seq=%d", i, payloadSeq(rec))
		}
	}
	seq, err := w2.Append(fixedPayload(4, 12))
	if err != nil || seq != 4 {
		t.Fatalf("恢复后追加 seq=%d err=%v", seq, err)
	}
}

// TestFaultBeforeAnyByte：注入失败但一个字节都没写，不应有毒化副作用。
func TestFaultBeforeAnyByte(t *testing.T) {
	dir := t.TempDir()
	var failed bool
	opt := Options{
		SegmentBytes: 1 << 20,
		MaxBytes:     1 << 20,
		Fault: FaultHooks{
			BeforeFrameWrite: func(seq uint64) (int, error) {
				if seq == 2 && !failed {
					failed = true
					return 0, errors.New("nope")
				}
				return 0, nil
			},
		},
	}
	w := reopen(t, dir, opt)
	if _, err := w.Append(fixedPayload(1, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(fixedPayload(2, 8)); err == nil {
		t.Fatal("注入的错误没有透传")
	}
	seq, err := w.Append(fixedPayload(2, 8))
	if err != nil || seq != 2 {
		t.Fatalf("零字节失败后应可继续：seq=%d err=%v", seq, err)
	}
}

// TestConcurrentAppendOrder：多个 goroutine 抢当前段，
// 返回的序号构成 1..N 的排列；日志对外可见顺序按序号。
func TestConcurrentAppendOrder(t *testing.T) {
	dir := t.TempDir()
	w := reopen(t, dir, Options{SegmentBytes: 100, MaxBytes: 1 << 20})

	const writers = 8
	const per = 50
	var wg sync.WaitGroup
	seqs := make(chan uint64, writers*per)
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				seq, err := w.Append(fixedPayload(0, 16))
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				seqs <- seq
			}
		}()
	}
	wg.Wait()
	close(seqs)

	seen := map[uint64]bool{}
	for s := range seqs {
		if seen[s] {
			t.Fatalf("重复序号 %d", s)
		}
		seen[s] = true
	}
	if len(seen) != writers*per {
		t.Fatalf("序号数 = %d, want %d", len(seen), writers*per)
	}
	for s := uint64(1); s <= uint64(writers*per); s++ {
		if !seen[s] {
			t.Fatalf("缺序号 %d", s)
		}
	}

	got := readAll(t, w)
	if len(got) != writers*per {
		t.Fatalf("磁盘记录数 = %d, want %d", len(got), writers*per)
	}
	// 快照吐出的记录必须严格按段内物理顺序（= 序号顺序）。
	for i, rec := range got {
		// 记录内容里没带序号（payload 固定），用帧数和跨段连续性校验顺序：
		if len(rec) != 16 {
			t.Fatalf("记录 %d 长度错", i)
		}
	}

	// 直接扫盘上的帧头：跨段读出的帧序号必须是 1..N 严格递增，
	// 证明并发抢同一段时“后面的”绝不可能先落盘露出来。
	var frameSeqs []uint64
	for _, s := range w.segs {
		f, err := os.Open(s.path)
		if err != nil {
			t.Fatal(err)
		}
		info, _ := f.Stat()
		var off int64
		hdr := make([]byte, frameHeader)
		for off < info.Size() {
			seq, _, advance, err := readFrame(f, off, info.Size(), hdr)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				t.Fatalf("扫帧失败: %v", err)
			}
			frameSeqs = append(frameSeqs, seq)
			off += advance
		}
		f.Close()
	}
	if len(frameSeqs) != writers*per {
		t.Fatalf("盘上帧数 = %d, want %d", len(frameSeqs), writers*per)
	}
	for i, seq := range frameSeqs {
		if seq != uint64(i+1) {
			t.Fatalf("盘上可见顺序错位：位置 %d 帧序号 %d", i+1, seq)
		}
	}
}

// TestReaderVisibilityFollowsSeq：序号 k 的帧没落盘完之前，
// 快照不可能看到 k；写完成后拿到的快照是 1..k 的前缀，不会露出后面的。
func TestReaderVisibilityFollowsSeq(t *testing.T) {
	dir := t.TempDir()
	w := reopen(t, dir, Options{SegmentBytes: 1 << 20, MaxBytes: 1 << 20})

	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := uint64(1); seq <= 10; seq++ {
			if seq == 6 {
				<-release // 人为让第 6 条还没开始
			}
			if _, err := w.Append(fixedPayload(seq, 10)); err != nil {
				t.Errorf("append: %v", err)
				return
			}
		}
	}()

	// 等前 5 条完成。mu 的持有者串行化追加，用轮询读到恰好 5 条为止。
	deadline := false
	for i := 0; i < 1000 && !deadline; i++ {
		r, err := w.NewReader()
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for {
			_, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			count++
		}
		r.Close()
		if count == 5 {
			deadline = true
		}
	}
	if !deadline {
		t.Fatal("等不到稳定的 5 条前缀快照")
	}

	// 此刻快照永远是 1..5，不可能看到 6 及以后（“后面的不能先露出来”）。
	r, _ := w.NewReader()
	count := 0
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
		if payloadSeq(rec) != uint64(count) {
			t.Fatalf("可见顺序错位：第 %d 条 seq=%d", count, payloadSeq(rec))
		}
	}
	r.Close()
	if count != 5 {
		t.Fatalf("第 6 条未落盘却露出了后面的记录：%d 条", count)
	}
	close(release)
	wg.Wait()
}

// TestSegmentRollover：写满当前段后封口、开新段，记录跨段按序可读。
func TestSegmentRollover(t *testing.T) {
	dir := t.TempDir()
	w := reopen(t, dir, Options{SegmentBytes: 60, MaxBytes: 1 << 20})

	const n = 12
	for i := 1; i <= n; i++ {
		if _, err := w.Append(fixedPayload(uint64(i), 10)); err != nil {
			t.Fatal(err)
		}
	}

	var names []string
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if len(names) < 2 {
		t.Fatalf("段没有轮转，只有 %v", names)
	}

	got := readAll(t, w)
	if len(got) != n {
		t.Fatalf("跨段记录数 = %d, want %d", len(got), n)
	}
	for i, rec := range got {
		if payloadSeq(rec) != uint64(i+1) {
			t.Fatalf("跨段顺序错位：位置 %d seq=%d", i, payloadSeq(rec))
		}
	}

	// 已封口段不能再有写入句柄；文件内容落盘且重新打开后一致。
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w = reopen(t, dir, Options{SegmentBytes: 60, MaxBytes: 1 << 20})
	if got := readAll(t, w); len(got) != n {
		t.Fatalf("重开后跨段记录数 = %d, want %d", len(got), n)
	}
}

// TestDiskLimitDropsOldestSealed：超过占用上限时，
// 最老的已封口段可以被删除；未封口的活动段绝不删除。
func TestDiskLimitDropsOldestSealed(t *testing.T) {
	dir := t.TempDir()
	const segBytes = 64
	const maxBytes = 200
	w := reopen(t, dir, Options{SegmentBytes: segBytes, MaxBytes: maxBytes})

	for i := 1; i <= 40; i++ {
		if _, err := w.Append(fixedPayload(uint64(i), 10)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}

		entries, _ := os.ReadDir(dir)
		var total int64
		activeExists := false
		for _, e := range entries {
			info, err := e.Info()
			if err != nil {
				t.Fatal(err)
			}
			total += info.Size()
			activeExists = true
		}
		if len(entries) == 0 || !activeExists {
			t.Fatal("活动段被删除了")
		}
		// 上限允许被单帧/活动段短暂超过，但多出来的必须只是已封口段被清理后的残量。
		if total > maxBytes+frameBytes(10) {
			t.Fatalf("第 %d 条后占用 %d 远超上限 %d", i, total, maxBytes)
		}

		w.mu.Lock()
		for j, s := range w.segs {
			if j < len(w.segs)-1 && !s.sealed() {
				t.Fatal("非活动段没有封口")
			}
			if _, err := os.Stat(s.path); err != nil {
				t.Fatalf("跟踪中的段 %s 已丢失: %v", s.path, err)
			}
		}
		active := w.segs[len(w.segs)-1]
		if active.sealed() {
			t.Fatal("活动段被封口了")
		}
		w.mu.Unlock()
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) == 1 {
		// 清理到只剩活动段是允许的；确认它仍然可读且未被删。
		if _, err := os.Stat(filepath.Join(dir, entries[0].Name())); err != nil {
			t.Fatal(err)
		}
	}

	// 重新打开后，存活段里的记录依然严格按序号、无错位。
	w = reopen(t, dir, Options{SegmentBytes: segBytes, MaxBytes: maxBytes})
	got := readAll(t, w)
	for i := 1; i < len(got); i++ {
		if payloadSeq(got[i]) != payloadSeq(got[i-1])+1 {
			t.Fatalf("淘汰老段后序号不连续：%d -> %d", payloadSeq(got[i-1]), payloadSeq(got[i]))
		}
	}
	if seq := payloadSeq(got[len(got)-1]); seq != 40 {
		t.Fatalf("最新记录 seq=%d，应保留到 40", seq)
	}
}

func frameBytes(payload int) int64 { return int64(frameHeader + payload) }

// TestCloseRejectsAppend：关闭后再追加/读取要明确报错。
func TestCloseRejectsAppend(t *testing.T) {
	dir := t.TempDir()
	w := reopen(t, dir, Options{SegmentBytes: 64, MaxBytes: 1 << 20})
	if _, err := w.Append(fixedPayload(1, 8)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(fixedPayload(2, 8)); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	if _, err := w.NewReader(); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}

	// 关闭后重新打开，记录仍在且序号正确。
	w = reopen(t, dir, Options{SegmentBytes: 64, MaxBytes: 1 << 20})
	got := readAll(t, w)
	if len(got) != 1 || payloadSeq(got[0]) != 1 {
		t.Fatalf("重开记录异常: %v", got)
	}
}

// TestActiveSegmentNeverDeleted：只有活动段且其自身超过上限时，也不能删。
func TestActiveSegmentNeverDeleted(t *testing.T) {
	dir := t.TempDir()
	w := reopen(t, dir, Options{SegmentBytes: 1 << 30, MaxBytes: 64})
	for i := 1; i <= 5; i++ {
		if _, err := w.Append(fixedPayload(uint64(i), 30)); err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("活动段不该被删，现存 %v", entries)
	}
	if len(readAll(t, w)) != 5 {
		t.Fatal("活动段记录应全部保留")
	}
}

// TestRecordLargerThanCap：单条记录超过总盘上限直接报错，不留半截。
func TestRecordLargerThanCap(t *testing.T) {
	dir := t.TempDir()
	w := reopen(t, dir, Options{SegmentBytes: 1 << 10, MaxBytes: 64})
	if _, err := w.Append(make([]byte, 65)); !errors.Is(err, ErrDataTooLarge) {
		t.Fatalf("want ErrDataTooLarge, got %v", err)
	}
	if got := readAll(t, w); len(got) != 0 {
		t.Fatalf("被拒记录不应落盘，读到 %d 条", len(got))
	}
}

// TestReaderSnapshotSurvivesDeletion：早拿到的读取器在老段被删除后，
// 仍能把快照时已存在的记录按原顺序读完。
func TestReaderSnapshotSurvivesDeletion(t *testing.T) {
	dir := t.TempDir()
	w := reopen(t, dir, Options{SegmentBytes: 60, MaxBytes: 200})
	for i := 1; i <= 6; i++ {
		if _, err := w.Append(fixedPayload(uint64(i), 10)); err != nil {
			t.Fatal(err)
		}
	}
	r, err := w.NewReader()
	if err != nil {
		t.Fatal(err)
	}

	// 继续写入触发轮转和最老封口段的物理删除。
	for i := 7; i <= 30; i++ {
		if _, err := w.Append(fixedPayload(uint64(i), 10)); err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("老快照读取失败: %v", err)
		}
		count++
		if payloadSeq(rec) != uint64(count) {
			t.Fatalf("老快照顺序错位：位置 %d seq=%d", count, payloadSeq(rec))
		}
	}
	r.Close()
	if count != 6 {
		t.Fatalf("快照应读完当时的 6 条，实际 %d", count)
	}
}

// TestTornTailAcrossReopenReplaysInOrder：封口段里的半截帧在重放时被丢弃，
// 活动段半截尾巴被截断，完好记录跨段按原顺序吐出。
func TestTornTailAcrossReopenReplaysInOrder(t *testing.T) {
	dir := t.TempDir()
	opt := Options{SegmentBytes: 1 << 20, MaxBytes: 1 << 20}
	w := reopen(t, dir, opt)
	for i := 1; i <= 5; i++ {
		if _, err := w.Append(fixedPayload(uint64(i), 9)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// 手工给唯一的封口段末尾接一段垃圾，模拟任何“写了一半”的残留。
	path := filepath.Join(dir, segName(1))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("half-written-garbage")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	w = reopen(t, dir, opt)
	got := readAll(t, w)
	if len(got) != 5 {
		t.Fatalf("完好记录 = %d, want 5", len(got))
	}
	for i, rec := range got {
		if payloadSeq(rec) != uint64(i+1) {
			t.Fatalf("顺序错位：%d", payloadSeq(rec))
		}
	}
	info, _ := os.Stat(path)
	if info.Size() != 5*int64(frameHeader+9) {
		t.Fatalf("活动段尾巴未截断：%d", info.Size())
	}
}

func ExampleWAL() {
	dir, _ := os.MkdirTemp("", "wal")
	defer os.RemoveAll(dir)
	w, err := Open(dir, Options{SegmentBytes: 4096, MaxBytes: 1 << 20})
	if err != nil {
		return
	}
	seq, _ := w.Append([]byte("hello"))
	fmt.Println(seq)
	if err := w.Close(); err != nil {
		return
	}
	// Output:
	// 1
}
