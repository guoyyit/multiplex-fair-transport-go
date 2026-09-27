# multiplex-fair-transport-go

本地多路复用传输层：在**单条底层连接**上承载多条逻辑流，提供基于
**优先级 + 权重的加权公平调度**、逐流窗口背压、半关闭 / 优雅停机、
连接级心跳、空闲超时、可配置重连以及检查点崩溃恢复。底层连接由
内存管道（`transport.Pipe`）模拟，不依赖任何外部服务。

## 包结构

| 包 | 职责 |
|----|------|
| `frame` | 二进制帧编解码、magic 校验、畸形帧检测 |
| `transport` | 底层 `Conn` 抽象 + 内存管道（关闭前排空缓冲，不出半截帧） |
| `stream` | 逻辑流：有序交付、发送窗口、背压、半/全关闭状态机 |
| `scheduler` | stride 加权公平调度（确定性、无饿死） |
| `session` | 多路复用会话：握手、收发循环、心跳、重连、停机、恢复 |
| `checkpoint` | 收发偏移的原子持久化（内存 / JSON 文件） |
| `config` | 全部配置项与默认值 |

## 快速示例

```go
a, b := transport.Pipe()
cfg := config.Defaults()           // 或自定义后 cfg.Normalize()
client := session.New(a, cfg, true, nil)   // client=true -> 奇数流 id
server := session.New(b, cfg, false, nil)  // server=false -> 偶数流 id

// 服务端
go func() {
    st, _ := server.AcceptStream()
    data, _ := io.ReadAll(st)      // 严格按序交付，FIN -> io.EOF
    _, _ = st.Write(data)
    _ = st.CloseWrite()            // 半关闭写方向
}()

// 客户端
st, _ := client.OpenStream(context.Background(), priority, weight)
_, _ = st.Write([]byte("hello"))
_ = st.CloseWrite()
reply, _ := io.ReadAll(st)
```

## 关键保证

- **有序交付**：同一流按 seq 交付；重复帧去重，乱序（缺口）帧触发确定性复位。
- **逐流背压**：不读的对端会把发送方窗口压满，发送内存有界（4×MaxFramePayload），
  慢流只阻塞自身 writer，不拖累快流。
- **公平调度**：权重决定长期份额，优先级为有限倍率；同输入结果确定，低优先级不饿死。
- **优雅停机**：`Close()` 先按调度排空在途数据与 FIN，受 `DrainTimeout` 约束，
  超时后整批取消；`CloseNow()` 立即取消。
- **心跳 / 空闲 / 重连**：PING/PONG 保活，`IdleTimeout` 检测死连接，
  `Reconnect` 策略可配置并自动重放在途帧。
- **崩溃恢复**：偏移原子落盘，`RecoverStream` 从 SendNext / RecvDelivered 续传续收，
  不重复、不丢确认数据、不留部分写入。

## 文档

- [帧格式](docs/frame-format.md)
- [调度语义](docs/scheduling.md)
- [配置项 / 重连 / 检查点](docs/configuration.md)

## 测试

```sh
go test ./... -race
```

覆盖：慢速流背压、优先级 / 权重公平与确定性、半关闭与优雅停机、
超时取消、畸形帧与乱序拒绝、心跳空闲、重连策略、检查点崩溃恢复。
