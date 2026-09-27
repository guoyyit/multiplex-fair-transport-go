# multiplex-fair-transport-go

一个本地多路复用传输层：单条底层连接（`net.Conn`，可用本地回环或
`net.Pipe` 内存管道）承载多条逻辑流，并提供基于优先级与权重的
加权公平调度。不依赖任何外部服务。

```go
c1, c2 := net.Pipe()
client, _ := mux.Dial(c1, mux.DefaultConfig())
server, _ := mux.Listen(c2, mux.DefaultConfig())

st, _ := client.OpenStream(0 /*优先级*/, 1 /*权重*/)
peer, _ := server.AcceptStream()
st.Write([]byte("hello"))
st.CloseWrite()          // 单向半关闭
data, _ := io.ReadAll(peer)
```

## 帧格式

所有整数为大端序，定长 22 字节头部：

| 偏移 | 字段      | 类型   | 说明                                   |
|-----:|-----------|--------|----------------------------------------|
|  0   | magic     | uint16 | 固定 `0x4D46`                          |
|  2   | version   | uint8  | 固定 `1`                               |
|  3   | type      | uint8  | 帧类型（见下）                         |
|  4   | flags     | uint16 | 保留                                   |
|  6   | streamID  | uint32 | 流编号（客户端奇数、服务端偶数）       |
| 10   | seq       | uint64 | 序号（DATA 为流内字节偏移；WINDOW_UPDATE 为已交付确认偏移） |
| 18   | length    | uint32 | 载荷长度，硬上限 1 MiB                 |
| 22   | payload   | bytes  | 载荷                                   |

帧类型：`SYN`(1) `DATA`(2) `FIN`(3) `RST`(4) `WINDOW_UPDATE`(5)
`PING`(6) `PONG`(7) `GOAWAY`(8)。

- `SYN` 载荷 5 字节：`priority uint8` + `weight uint32`，`seq` 为发送起始偏移（恢复用）。
- `WINDOW_UPDATE` 载荷 4 字节：流量控制信用（字节数）；`seq` 为接收方应用已交付的累积偏移（确认）。
- `DATA` 的 `seq` 是该载荷第一个字节在流内的偏移。

**写入原子性**：每个帧先完整序列化到内存，再由唯一的发送 goroutine
一次性写出；任何失败都不会在线上产生半截帧。截断的帧在读取侧以
`io.ErrUnexpectedEOF` 拒绝，绝不会按部分帧交付。

**畸形帧策略**：

| 情况 | 处理 |
|------|------|
| magic/version/类型非法、长度超限 | 连接级致命：连接关闭并上报 `ErrMalformedFrame` |
| 未知流的 `DATA` | 流级：回 `RST`，连接保持 |
| 流内序号空洞（乱序） | 流级：拒绝并 `RST` 该流 |
| 完全/部分重复（恢复后重发） | 丢弃或裁剪重叠前缀，不重复交付 |
| 接收缓冲或发送窗口溢出 | 流级：`RST`（`ErrWindowOverflow`） |

## 流语义

- **按序交付**：同一流内数据严格按序、恰好一次交付给 `Read`。
- **背压**：每条流有独立发送窗口（`SendWindow`）与发送队列上限
  （等于窗口）。窗口耗尽时 `Write` 阻塞；接收方应用每 `Read` 消费
  n 字节即回送 n 字节信用。慢速流只阻塞自身，且每流缓冲有界
  （≤ 窗口 + 接收缓冲），不会拖垮快速流或造成无界缓冲。
- **半关闭**：`CloseWrite` 发送 `FIN`（排在已缓冲数据之后，在途数据
  先排空）；对端 `Read` 读尽缓冲后得到 `io.EOF`，其写方向仍可用。
- **双向关闭**：`Close` 关闭两个方向；之后到达的数据被 `RST` 拒绝。
- **取消**：`Reset` / 连接关闭使流进入终态，阻塞中的 `Read`/`Write`
  立即返回错误，流不会停留在中间状态。
- **截止时间**：`SetDeadline` / `SetReadDeadline` / `SetWriteDeadline`，
  超时返回 `ErrTimeout`。

## 调度语义

发送侧采用**平滑加权轮询（SWRR）**：

- 有效权重 = `weight × (8 - priority)`，`priority ∈ [0,7]`，0 最高。
- 每个就绪流每轮 `current += 有效权重`，选 `current` 最大者发出一帧，
  胜者 `current -= 总权重`；平局取流编号小者。
- 全部整数运算：**相同权重与输入下，调度结果逐字节确定**。
- 每个优先级都保留正权重，因此高优先级流**永远不会永久饿死**低优先级
  流：优先级 0 与 7 的长期帧数比恰为 8:1（测试验证）。
- 调度器只在多条流同时有数据且窗口可用时决定帧的混合比例；单流
  不受其他流影响。

## 连接管理

- **心跳**：按 `HeartbeatInterval` 发送 `PING`，对端回 `PONG`。
- **空闲超时**：任意帧到达都会刷新活动时间；超过 `IdleTimeout` 无帧
  到达则连接以 `ErrIdleTimeout` 关闭。心跳流量可使空闲连接保活。
- **优雅停机**：`Shutdown(ctx)` 发送 `GOAWAY`（双方停止开新流），
  等待所有流的发送队列排空且被对端确认；超过 `DrainTimeout`（或 ctx
  取消）则取消：连接关闭，所有流进入终态而非半排空状态。
- **错误上报**：`Config.OnError` 回调接收异步连接错误（畸形帧、空闲
  超时、底层 IO 错误），可用于触发重连等策略；主动 `Close` 不上报。

## 检查点与崩溃恢复

`Conn.Checkpoint()` 捕获每条流的四个偏移（均为帧边界，不会留下部分
写入）：

- `SendOffset`：下一个待发送序号；
- `SendAcked`：对端应用已确认（已 `Read`）的偏移；
- `RecvOffset`：本地应用已交付的偏移；
- `Closed`：流是否已终结（终结流不恢复）。

`Store` 接口提供 `MemStore` 与 `FileStore`（JSON，临时文件 + rename
原子写入，崩溃不会留下写了一半的检查点）。

恢复流程：

1. 新连接调用 `Restore(cp)` 安装偏移；
2. 发送方用 `ResumeStream(id, priority, weight)` 以原流编号恢复，
   发送序号从 `SendAcked` 继续——**已确认数据绝不重发**；
3. 应用重发未确认数据（传输层不持久化应用数据）；
4. 接收方从 `RecvOffset` 继续交付；若出现重叠（重复）帧，按序号
   去重——**不重复交付、不丢失已确认数据**。

## 配置项

| 字段 | 默认 | 说明 |
|------|------|------|
| `MaxPayloadSize` | 16 KiB | 单帧载荷上限（硬上限 1 MiB） |
| `SendWindow` | 256 KiB | 每流发送窗口（在途未确认字节上限），亦是发送队列上限 |
| `RecvBuffer` | 256 KiB | 每流接收缓冲上限 |
| `HeartbeatInterval` | 5 s | 心跳周期；负值禁用 |
| `IdleTimeout` | 15 s | 空闲超时；负值禁用；启用时应 ≥ 2×心跳周期 |
| `DrainTimeout` | 5 s | 优雅停机排空时限；负值禁用（仅受 ctx 约束） |
| `MaxStreams` | 256 | 并发流上限 |
| `OnError` | nil | 异步错误回调（重连/上报策略钩子） |

## 测试

```sh
go test ./... -race
```

覆盖：按序交付、半关闭、优雅停机排空、慢速流背压（有界缓冲 + 不阻塞
快速流）、写截止时间、调度器确定性与加权分布、优先级公平（8:1 且不
饿死）、畸形/截断/未知流/乱序/重复帧、空闲超时、心跳保活、停机超时
取消、检查点崩溃恢复与 FileStore 原子性。
