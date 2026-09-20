# wal

从零实现的分段预写日志（write-ahead log），只用 Go 标准库，无第三方包。

## 帧格式

每条记录写成一个自描述帧（小端序）：

```
magic(4) | seq(8) | length(4) | crc32(seq+payload)(4) | payload(length)
```

`seq` 是全局单调序号（从 1 开始），CRC-32(IEEE) 覆盖序号和载荷。

## 语义

- **分段**：写入先追加到当前活动段；活动段字节数达到 `SegmentBytes`
  后先 fsync、封口（关闭句柄），再建新段。
- **坏记录**：帧界完整但 magic/CRC 不对的整条跳过，后续好记录按各自
  帧里的序号吐出，不错位。
- **崩溃恢复**：重新 `Open` 时，活动段末尾写了一半的帧被 `truncate`
  丢掉；已完整落盘的帧跨段按原顺序读出。
- **并发**：所有追加在一把互斥锁内完成“整帧写入 + fsync + 推进序号”，
  对外可见顺序严格等于序号顺序；`NewReader` 取的是段快照。
- **占盘上限**：总字节超过 `MaxBytes` 时，从最老的已封口段开始删除；
  只有活动段（未封口）时即使超限也保留，绝不删除。

## 用法

```go
w, err := wal.Open(dir, wal.Options{SegmentBytes: 4 << 10, MaxBytes: 64 << 20})
seq, err := w.Append(payload)      // 追加，返回序号

r, _ := w.NewReader()              // 快照式顺序读
for {
    rec, err := r.Next()           // io.EOF 表示读完
    ...
}
```

## 测试

测试用固定字节构造确定性载荷，并通过 `Options.Fault.BeforeFrameWrite`
注入“写了 n 个字节后失败”等故障点模拟半截写入，不杀进程：

```
go test -race ./...
```
