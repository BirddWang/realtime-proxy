Realtime-Proxy FrontEnd Docs
===

Connection
---
- WebSocket URL: `wss://ilalab.ai.cgu.edu.tw/realtime/ws`
- Query Parameters:
    - `raw=0`: `server` 會回傳帶有`Generation ID` 的資料，用於處理打斷邏輯。
    - `raw=1`: 只回傳PCM音訊，無法處理插話打斷

Audio Format
---
- Sample Rate: `24_000 Hz`
- Bit Depth: `16-bit` (PCM16)
- Channels: `Mono`
- Endianness: `Little-Endian`


上行訊息 \[Client -> Server\]
---

### A. Binary Message
- 將收到的音訊Resample至24K PCM16 `ArrayBuffer`
- 持續發送（Streaming）。
    - suggestion: 20ms ~ 50ms 發送一次

### B. Text Message
- use JSON String to control
    - "cancel" //強制打斷 AI 說話
    - "clear" // 清空 Buffer


下行訊息 \[Server -> Client\]
---

### A. Text Message
```json=
{
  "type": "playback.interrupt",
  "gen": 17392831,       // [關鍵] 世代 ID (BigInt)
  "reason": "speech_started"
}
```

### B. Binary Message
| Type (1B) | Generation ID (8B)      | Audio Payload (N Bytes) |
|-----------|-------------------------|-------------------------|
| 0x01      | uint64 (Little Endian)  | PCM16 Data...           |

- Byte 0 `0x01` 固定標誌
- Byte 1-8 `Generation ID` 該音訊的編號（編號大小決定新舊，越新越大）
- Byte 9-End: PCM Audio

Authentication
---
- 使用 HTTP Basic Auth
- WebSocket 連線時需在 URL 中帶入帳密，或透過 HTTP Header
- 範例：`wss://<USERNAME>:<PASSWORD>@ilalab.ai.cgu.edu.tw/realtime/ws`