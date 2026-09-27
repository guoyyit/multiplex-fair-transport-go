# 配置项（Configuration）

通过 `config.Config` 传入 `session.New`，所有字段取零值时由
`config.Defaults()` / `Normalize()` 填充默认值。

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `InitialWindow` | 64 KiB | 每条流的接收窗口（字节），通过 OPEN 握手通告对端。|
| `MaxFramePayload` | 16 KiB | 单个 DATA 帧的最大载荷；发送端内存上限取其 4 倍。|
| `AcceptQueue` | 64 | 待 Accept 的入站流缓冲；满时新流被复位。|
| `HeartbeatInterval` | 15s | PING 间隔；设为 0 或负值则关闭心跳与空闲检测（测试常用）。|
| `IdleTimeout` | 30s | 若该时间内未收到任何帧（含 PONG），判定连接死亡。|
| `DrainTimeout` | 5s | 优雅停机时在途数据的最长排空时间，超时后取消剩余流。|
| `Reconnect` | ReconnectNone | 故障策略：None 直接失败；Once 重连一次；Always 按退避持续重连。|
| `MaxRetries` | 0（不限） | ReconnectAlways 的最大重连次数。|
| `ReconnectBackoff` | 100ms | 两次重连之间的等待。|
| `CheckpointPath` | 空（关闭） | 检查点 JSON 文档路径；置空则不持久化。也可直接传入 `checkpoint.Store`。|

## 故障与重连策略（可配置且可解释）

- `ReconnectNone`：连接断开 / 空闲超时 / 畸形帧 → 立即拆除会话，
  全部流收到 `ErrCancelled`（畸形帧）或断开错误，不做半截处理。
- `ReconnectOnce`：底层连接失败后用 `Dialer` 新建连接，重放未确认帧，
  随后仍失败则按 None 处理。
- `ReconnectAlways`：按 `ReconnectBackoff` 退避持续重连，最多 `MaxRetries` 次
  （0 为不限），直到会话被显式关闭。重连后对仍存活的流重放 replay 日志中的
  在途帧；重复帧由对端按 seq 去重，不产生二次交付。

使用 `session.NewWithDialer(dialer, cfg, client, store)` 提供建连函数即可启用重连。

## 检查点与崩溃恢复

- 会话在打开流、发送 DATA（推进 SendNext）、收到 ACK（推进 SendAcked）、
  接收数据（RecvNext）与 reader 消费（RecvDelivered）时把偏移原子写入检查点。
- 文件存储采用“写临时文件 + fsync + rename”，崩溃只会看到上一份完整文档或
  新文档，绝不会留下部分写入。
- **仅当流干净关闭（数据全部确认并完成 FIN 握手）才删除检查点**；取消或复位
  保留偏移，供重启后 `Session.RecoverStream(id, priority, weight)` 恢复。
- 恢复后发送侧从 `SendNext` 续编号（不重用已确认序号），接收侧从
  `RecvNext/RecvDelivered` 续交付（不重复投递已消费字节）。

## 心跳与空闲检测

心跳启用时（`HeartbeatInterval > 0`），`heartbeatLoop` 周期性发送 PING，并以 `IdleTimeout/2` 的频率检查
`lastActive`（任意入站帧都会刷新，包括 PONG）。超时未活动即触发
`handleConnFailure(idle timeout)`，按上面的 Reconnect 策略处理。
帧头自带 magic 与长度，异常断开后内存管道会先排空已写入的完整帧再返回 EOF，
不会把半截帧交付给上层。
