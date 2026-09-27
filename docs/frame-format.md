# 帧格式（Wire Frame Format）

所有数字均使用大端序（big-endian）。每条帧由固定 20 字节帧头与可选载荷组成：

```
偏移  长度  字段        说明
0     2    magic       固定 0x4D 0x58（"MX"），用于检测字节流失步，
                       失步即判定为致命畸形帧，杜绝“半截帧被当作数据”。
2     1    type        帧类型，见下表。
3     1    flags       位标志；CLOSE 帧使用 CloseFlag，OPEN 帧用 bit0 表示 ACK。
4     4    stream_id   流编号，uint32。客户端取奇数，服务端取偶数，互不冲突。
8     8    seq         序列号/偏移，uint64：
                       - DATA：该载荷首字节在流中的字节序号；
                       - ACK ：累计确认，下一期望字节序号；
                       - WINDOW_UPDATE：本次归还的信用字节数；
                       - CLOSE：FIN 所在的字节序号；
                       - PING/PONG：随机 nonce。
16    4    length      载荷字节数，最大 MaxPayloadSize（1 MiB）。
20    N    payload     长度为 length 的载荷。
```

## 帧类型

| type | 名称            | 方向 | 载荷 | 语义 |
|------|-----------------|------|------|------|
| 0x01 | DATA            | 双向 | 数据 | 携带 stream_id 与字节序号 seq。|
| 0x02 | ACK             | 双向 | 空   | 累计确认到 seq（不含）。只表示“已收到”，不释放发送许可。|
| 0x03 | WINDOW_UPDATE   | 双向 | 空   | 接收方的 reader 消费了 seq 字节，向发送方归还等量信用。|
| 0x04 | OPEN            | 双向 | 16B  | 打开流。载荷：priority(4) + weight(4) + window(8)。flags bit0=1 表示握手 ACK。|
| 0x05 | CLOSE           | 双向 | 空   | flags：0x01=关闭读，0x02=FIN 关闭写，0x03=双向关闭/复位。|
| 0x06 | PING            | 双向 | 空   | 心跳请求，seq=nonce。|
| 0x07 | PONG            | 双向 | 空   | 心跳响应，原样回带 nonce。|

OPEN 的 16 字节载荷：

```
priority uint32 | weight uint32 | initial_window uint64
```

发起方在 OPEN 中告知自己的接收窗口；接收方回一帧 `flags=ACK` 的 OPEN，
携带其接收窗口。流的发送侧在收到该 ACK 之前窗口为 0，不能发送数据。

## 乱序、重复与畸形帧策略

- **乱序（seq 缺口）**：接收方维护 `recvNext`。`seq == recvNext` 才入缓冲；
  `seq > recvNext` 立即判定缺口，向对端发送 CLOSE(CloseBoth) 并本地复位该流，
  缺口之前已连续到达的数据仍可读，流不会停在中间状态。
- **重复（seq < recvNext）**：视为重传副本，丢弃载荷并重新回 ACK，绝不二次交付。
- **畸形帧**：magic 错误、未知类型、length 超过上限，或缓冲不足以成帧时，
  不足返回 `ErrShortBuffer` 等待更多字节；确定畸形则连接致命，会话拆除并取消全部流。

## 流控（流量控制）

- 接收窗口仅约束**在途未归还信用**的字节数 `inFlight <= sendWindow`。
- 初始窗口通过 OPEN 握手通告；接收方只有在 reader **实际消费**数据后才发送
  WINDOW_UPDATE 归还信用。因此对端不读时发送方被真实背压。
- ACK 只推进确认/重放边界 `sendBase`，**不**释放发送许可。
- 发送方额外以 `maxQueue = 4 × MaxFramePayload` 限制“在途 + 排队”总内存，
  慢速流最多阻塞自己的 writer，不会造成无界缓冲，也不会阻塞其他流。
