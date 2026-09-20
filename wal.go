// Package wal 实现了一个分段的预写日志（write-ahead log）。
//
// 日志由若干固定大小上限的段文件组成，任何时刻只有一个未封口的活动段
// 接收追加。每条记录自成一帧，帧头带序号、长度和 CRC-32 校验和：
//
//	magic(4) | seq(8) | length(4) | crc32(seq+payload)(4) | payload(length)
//
// 除标准库外不依赖任何第三方包。
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
	"sync"
)

const (
	frameHeader = 4 + 8 + 4 + 4
	frameMagic  = 0x57_41_4C_00 // "WAL\0"
	segPrefix   = "segment-"
	segSuffix   = ".wal"
)

var (
	// ErrClosed 表示日志已关闭，不能再追加。
	ErrClosed = errors.New("wal: closed")
	// ErrRecoverNeeded 表示本进程内一次追加写出了半截帧，
	// 该实例被毒化，必须重新 Open 截断尾巴后才能继续。
	ErrRecoverNeeded = errors.New("wal: torn frame, reopen to recover")
	// ErrDataTooLarge 表示单条记录本身就超过了磁盘上限，放不下。
	ErrDataTooLarge = errors.New("wal: record larger than max bytes")
)

var errBadCRC = errors.New("wal: checksum mismatch")

// Options 控制段大小与磁盘占用上限。
type Options struct {
	// SegmentBytes 是段被封口的字节阈值；触发封口的这一帧可以超过阈值。
	SegmentBytes int64
	// MaxBytes 是所有段文件的总字节软上限。超出时只允许扔最老的已封口段；
	// 只有活动段（未封口）时即使超限也不会删它。
	MaxBytes int64

	// Fault 是测试用的可注入失败点，正常使用留零值。
	Fault FaultHooks
}

// FaultHooks 汇总可注入的故障，全部可为 nil。
type FaultHooks struct {
	// BeforeFrameWrite 在每帧写入前调用。返回 n>0 时，先向段文件
	// 写入编码帧的前 n 个字节，然后把注入的错误包在 ErrRecoverNeeded
	// 里返回，模拟写到一半被掐断；n==0 时不写任何字节。
	BeforeFrameWrite func(seq uint64) (n int, err error)
}

// segment 是磁盘上一个段文件的元信息。file == nil 表示已封口。
type segment struct {
	id   uint64
	path string
	size int64
	file *os.File
}

func (s *segment) sealed() bool { return s.file == nil }

// WAL 是一个可并发追加、按序号读取的分段预写日志。
type WAL struct {
	dir string
	opt Options

	mu     sync.Mutex
	segs   []*segment // 按 id 升序，最后一个是活动段
	next   uint64     // 下一条记录的序号，从 1 开始
	closed bool
	poison bool // 出现过半截帧，拒绝再写
}

// Open 打开（或创建）dir 下的日志，并执行崩溃恢复：
// 截断活动段末尾不完整的帧，跳过校验和损坏但帧界完整的记录。
func Open(dir string, opt Options) (*WAL, error) {
	if opt.SegmentBytes <= 0 {
		return nil, errors.New("wal: SegmentBytes must be positive")
	}
	if opt.MaxBytes <= 0 {
		return nil, errors.New("wal: MaxBytes must be positive")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		var id uint64
		if _, err := fmt.Sscanf(e.Name(), segPrefix+"%020d"+segSuffix, &id); err == nil {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	w := &WAL{dir: dir, opt: opt, next: 1}
	for _, id := range ids {
		w.segs = append(w.segs, &segment{
			id:   id,
			path: filepath.Join(dir, segName(id)),
		})
	}
	if err := w.recoverLocked(); err != nil {
		return nil, err
	}
	return w, nil
}

func segName(id uint64) string {
	return fmt.Sprintf("%s%020d%s", segPrefix, id, segSuffix)
}

// Append 追加一条记录，返回它被分配到的全局单调序号。
// 多 goroutine 并发调用时，对外可见顺序严格等于返回的序号顺序：
// 上一整帧写入并 fsync 完成、序号推进后，下一个写入者才开始。
func (w *WAL) Append(data []byte) (uint64, error) {
	if int64(len(data)) > w.opt.MaxBytes {
		return 0, ErrDataTooLarge
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	if w.poison {
		return 0, ErrRecoverNeeded
	}

	seq := w.next
	frame := encodeFrame(seq, data)

	active := w.segs[len(w.segs)-1]
	if active.size >= w.opt.SegmentBytes {
		if err := w.rollLocked(); err != nil {
			return 0, err
		}
		active = w.segs[len(w.segs)-1]
	}

	if hook := w.opt.Fault.BeforeFrameWrite; hook != nil {
		if n, err := hook(seq); err != nil {
			if n > 0 {
				if n > len(frame) {
					n = len(frame)
				}
				if _, werr := active.file.Write(frame[:n]); werr != nil {
					return 0, werr
				}
				active.size += int64(n)
				w.poison = true
				return 0, fmt.Errorf("%w: %v", ErrRecoverNeeded, err)
			}
			return 0, err
		}
	}

	if _, err := active.file.Write(frame); err != nil {
		w.poison = true
		return 0, err
	}
	if err := active.file.Sync(); err != nil {
		w.poison = true
		return 0, err
	}
	active.size += int64(len(frame))
	w.next = seq + 1

	if err := w.enforceLimitLocked(); err != nil {
		return 0, err
	}
	return seq, nil
}

// rollLocked 封住当前活动段并创建新段。
func (w *WAL) rollLocked() error {
	old := w.segs[len(w.segs)-1]
	if err := old.file.Sync(); err != nil {
		return err
	}
	if err := old.file.Close(); err != nil {
		return err
	}
	old.file = nil

	if err := w.createSegmentLocked(old.id + 1); err != nil {
		return err
	}
	return w.enforceLimitLocked()
}

// enforceLimitLocked 在总占用超限时删除最老的已封口段，可连续删除多个。
// 活动段（未封口）永远不会被删除。
func (w *WAL) enforceLimitLocked() error {
	var total int64
	for _, s := range w.segs {
		total += s.size
	}
	for total > w.opt.MaxBytes {
		idx := -1
		for i, s := range w.segs {
			if s.sealed() {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil // 只剩活动段，超限也不删
		}
		victim := w.segs[idx]
		if err := os.Remove(victim.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		total -= victim.size
		w.segs = append(w.segs[:idx], w.segs[idx+1:]...)
	}
	return syncDir(w.dir)
}

// Reader 是某一时刻日志的快照读取器，按段顺序、帧序号顺序吐记录。
type Reader struct {
	files []*os.File
	sizes []int64

	segIdx int
	off    int64
	hdr    []byte
}

// NewReader 对当前所有段做一致性快照。快照拿到后即使日志继续轮转、
// 删除最老段，也不影响本读取器把已有记录按原顺序读完。
func (w *WAL) NewReader() (*Reader, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrClosed
	}
	r := &Reader{hdr: make([]byte, frameHeader)}
	for _, s := range w.segs {
		f, err := os.Open(s.path)
		if err != nil {
			r.closeFiles()
			return nil, err
		}
		r.files = append(r.files, f)
		r.sizes = append(r.sizes, s.size)
	}
	return r, nil
}

// Next 返回下一条完好的记录，读完返回 io.EOF。
// 校验和损坏但帧界完整的记录被跳过，后续记录的序号不错位。
func (r *Reader) Next() ([]byte, error) {
	for r.segIdx < len(r.files) {
		if r.off >= r.sizes[r.segIdx] {
			r.segIdx++
			r.off = 0
			continue
		}
		_, payload, advance, err := readFrame(r.files[r.segIdx], r.off, r.sizes[r.segIdx], r.hdr)
		if err == io.EOF { // 段尾的半截帧不属于快照
			break
		}
		if err == errBadCRC {
			if advance == 0 {
				// 帧头就坏了，本段剩余内容无法定位；后续在别的段里，继续。
				r.segIdx++
				r.off = 0
				continue
			}
			r.off += advance
			continue
		}
		if err != nil {
			return nil, err
		}
		r.off += advance
		return payload, nil
	}
	return nil, io.EOF
}

// Close 释放快照持有的文件句柄。
func (r *Reader) Close() error {
	return r.closeFiles()
}

func (r *Reader) closeFiles() error {
	var first error
	for _, f := range r.files {
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
	}
	r.files = nil
	return first
}

// readFrame 从 f 的 off 处读取一帧。
//
// 返回序号、payload（新分配副本）和本帧占用字节数。
// 段尾读不到完整帧头或完整载荷时返回 io.EOF；
// 帧界完整但 magic 不对或 CRC 不符时返回 errBadCRC 和可跳过的 advance。
func readFrame(f *os.File, off, size int64, hdr []byte) (seq uint64, payload []byte, advance int64, err error) {
	if off+int64(frameHeader) > size {
		return 0, nil, 0, io.EOF
	}
	if err := readFullAt(f, hdr, off); err != nil {
		return 0, nil, 0, err
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != frameMagic {
		return 0, nil, 0, errBadCRC
	}
	seq = binary.LittleEndian.Uint64(hdr[4:12])
	length := binary.LittleEndian.Uint32(hdr[12:16])
	sum := binary.LittleEndian.Uint32(hdr[16:20])

	total := int64(frameHeader) + int64(length)
	if off+total > size {
		return 0, nil, 0, io.EOF
	}
	data := make([]byte, length)
	if err := readFullAt(f, data, off+int64(frameHeader)); err != nil {
		return 0, nil, 0, err
	}
	var check [8]byte
	binary.LittleEndian.PutUint64(check[:], seq)
	if crc32.ChecksumIEEE(append(check[:], data...)) != sum {
		return seq, nil, total, errBadCRC
	}
	return seq, data, total, nil
}

func readFullAt(f *os.File, buf []byte, off int64) error {
	for len(buf) > 0 {
		n, err := f.ReadAt(buf, off)
		off += int64(n)
		buf = buf[n:]
		if err == io.EOF {
			if len(buf) == 0 {
				return nil
			}
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// createSegmentLocked 以读写方式创建 id 指定的新活动段。
func (w *WAL) createSegmentLocked(id uint64) error {
	path := filepath.Join(w.dir, segName(id))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	w.segs = append(w.segs, &segment{id: id, path: path, file: f})
	return syncDir(w.dir)
}

// recoverLocked 扫描所有段确定最大序号，并截断活动段的半截尾巴。
func (w *WAL) recoverLocked() error {
	if len(w.segs) == 0 {
		return w.createSegmentLocked(1)
	}

	var maxSeq uint64
	for i, s := range w.segs {
		f, err := os.Open(s.path)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		size := info.Size()

		var off int64
		hdr := make([]byte, frameHeader)
		for off < size {
			seq, _, advance, rerr := readFrame(f, off, size, hdr)
			if rerr == io.EOF {
				break // 段尾半截帧：封口段丢弃尾部，活动段要截断
			}
			if rerr == errBadCRC {
				if advance == 0 {
					// 帧头 magic 就坏了，无法确定帧界，剩余内容无法再定位。
					break
				}
				off += advance // 跳过整条坏记录，后面的记录序号不错位
				continue
			}
			if rerr != nil {
				f.Close()
				return rerr
			}
			if seq > maxSeq {
				maxSeq = seq
			}
			off += advance
		}
		f.Close()
		s.size = off

		if i == len(w.segs)-1 {
			// 活动段：丢掉写到一半的尾巴，再以读写方式打开继续追加。
			if off != size {
				if err := os.Truncate(s.path, off); err != nil {
					return err
				}
			}
			f, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
			if err != nil {
				return err
			}
			s.file = f
		}
	}

	w.next = maxSeq + 1
	return nil
}

// Close 封住并同步活动段。关闭后不能再追加。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	w.closed = true
	for _, s := range w.segs {
		if s.file != nil {
			if err := s.file.Sync(); err != nil {
				return err
			}
			if err := s.file.Close(); err != nil {
				return err
			}
			s.file = nil
		}
	}
	return nil
}

// encodeFrame 把一条记录编码成自描述帧。
func encodeFrame(seq uint64, data []byte) []byte {
	frame := make([]byte, frameHeader+len(data))
	binary.LittleEndian.PutUint32(frame[0:4], frameMagic)
	binary.LittleEndian.PutUint64(frame[4:12], seq)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(len(data)))
	var check [8]byte
	binary.LittleEndian.PutUint64(check[:], seq)
	sum := crc32.ChecksumIEEE(append(check[:], data...))
	binary.LittleEndian.PutUint32(frame[16:20], sum)
	copy(frame[frameHeader:], data)
	return frame
}

// syncDir fsync 目录，保证新建/删除的段文件目录项落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
