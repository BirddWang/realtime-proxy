package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

const (
	openAIRealtimeURL = "wss://nura-resource.cognitiveservices.azure.com/openai/realtime?api-version=2024-10-01-preview&deployment=gpt-realtime"

	// Realtime audio/pcm rate 最低 >= 24000（你已經踩過 16000 會被拒絕）
	rateHz = 24000
	ch     = 1

	// WS keepalive
	pongWait   = 30 * time.Second
	pingPeriod = 10 * time.Second
	writeWait  = 15 * time.Second // 增加到 15s，避免前端忙碌時超時
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type openAIEvent map[string]any

func main() {
	_ = godotenv.Load() // 沒有 .env 也沒關係

	if os.Getenv("OPENAI_API_KEY") == "" {
		log.Fatal("missing env OPENAI_API_KEY")
	}

	http.HandleFunc("/ws", handleClientWS)
	log.Println("listening on :8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleClientWS(w http.ResponseWriter, r *http.Request) {
	// raw=1: server->client 只送純 PCM binary（不送任何 Text 控制訊息/錯誤），方便用 websocat 直接存檔播放。
	// 預設（raw=0）會走 framed binary：1B kind + 8B gen + PCM payload。
	rawMode := r.URL.Query().Get("raw") == "1"
	framedBinary := !rawMode
	clientAcceptsText := !rawMode

	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer clientConn.Close()
	if rawMode {
		log.Println("client connected (raw=1)")
	} else {
		log.Println("client connected")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ---- client keepalive (重要：要跟其他 Write 共用同一把鎖，避免 concurrent write) ----
	var clientWriteMu sync.Mutex

	clientConn.SetReadLimit(8 * 1024 * 1024)
	clientConn.SetReadDeadline(time.Time{}) // 沒有讀取超時（ping/pong 會維持連線）
	clientConn.SetPongHandler(func(string) error {
		clientConn.SetReadDeadline(time.Time{}) // 重置為無超時
		return nil
	})
	go pingLoop(ctx, clientConn, &clientWriteMu)

	// ---- connect to OpenAI Realtime ----
	openaiConn, err := dialOpenAIRealtime()
	if err != nil {
		log.Println("dial openai error:", err)
		return
	}
	defer openaiConn.Close()

	// OpenAI read deadline / pong
	openaiConn.SetReadLimit(8 * 1024 * 1024)
	_ = openaiConn.SetReadDeadline(time.Now().Add(pongWait))
	openaiConn.SetPongHandler(func(string) error {
		_ = openaiConn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// ✅ 單一 writer：所有送給 OpenAI 的訊息（含 ping）都走這個 writer
	openaiWriter := NewWSWriter(ctx, openaiConn)

	// ---- session.update：開 server VAD + create_response=true（你就不用自己 commit/response.create）----
	openaiWriter.SendControl(openAIEvent{
		"type": "session.update",
		"session": openAIEvent{
			"instructions": "你是一個台灣人，請用台灣繁體中文、台灣口語習慣與使用者自然對話。避免使用中國大陸用語（如「視頻」「軟件」「信息」），改用台灣用語（如「影片」「軟體」「資訊」）。語氣親切自然，像台灣朋友聊天一樣。對話只需溫柔有愛心的簡短回覆就好。",
			"modalities": []string{"audio"},

			"input_audio_format":  "pcm16",
			"output_audio_format": "pcm16",

			"turn_detection": openAIEvent{
				"type":                "server_vad",
				"threshold":           0.5,
				"prefix_padding_ms":   300,
				"silence_duration_ms": 600,
			},

			"voice":                      "ash",
			"temperature":                0.8,
			"max_response_output_tokens": 4096,
		},
	})
	log.Println("→ session.update sent (server VAD enabled)")

	// ---- OpenAI receiver：收到 audio delta 就轉回 binary 給 client ----
	go func() {
		var gen uint64
		inSpeech := false
		allowAudio := true
		responseActive := false

		interruptPlayback := func(reason string) {
			gen++
			allowAudio = false

			if clientAcceptsText {
				msg, _ := json.Marshal(openAIEvent{
					"type":   "playback.interrupt",
					"gen":    gen,
					"reason": reason,
				})
				clientWriteMu.Lock()
				_ = clientConn.WriteMessage(websocket.TextMessage, msg)
				clientWriteMu.Unlock()
			}

			// 盡量讓 OpenAI 停掉舊 response（即使仍有少量尾包，前端也會丟掉）
			// 注意：若沒有 active response，直接 cancel 會回 response_cancel_not_active。
			if responseActive {
				openaiWriter.SendControl(openAIEvent{"type": "response.cancel"})
			}
		}

		framePCM := func(pcm []byte) []byte {
			// 1B kind(0x01 audio) + 8B gen (little-endian) + PCM payload
			buf := make([]byte, 1+8+len(pcm))
			buf[0] = 0x01
			binary.LittleEndian.PutUint64(buf[1:9], gen)
			copy(buf[9:], pcm)
			return buf
		}

		for {
			_, msg, err := openaiConn.ReadMessage()
			if err != nil {
				// 這通常是你 cancel / conn close 造成的，屬於正常收尾
				log.Println("openai read error:", err)
				cancel()
				return
			}

			var evt openAIEvent
			if err := json.Unmarshal(msg, &evt); err != nil {
				log.Println("openai json error:", err)
				continue
			}

			t, _ := evt["type"].(string)

			switch t {
			case "input_audio_buffer.speech_started":
				// 使用者開始說話：立刻打斷 client 播放並丟掉舊音訊
				log.Println("openai event:", t)
				if !inSpeech {
					inSpeech = true
					interruptPlayback("speech_started")
				}

			case "input_audio_buffer.speech_stopped":
				log.Println("openai event:", t)
				inSpeech = false
				// 若 response 已經 created（少數情境可能先到），就可恢復轉發
				if responseActive {
					allowAudio = true
				}

			case "response.created":
				log.Println("openai event:", t)
				responseActive = true
				if !inSpeech {
					allowAudio = true
				}

			case "error":
				pretty, _ := json.MarshalIndent(evt, "", "  ")
				log.Printf("❌ openai error event:\n%s\n", string(pretty))

				// 把 error 也丟回 client（文字）
				if clientAcceptsText {
					clientWriteMu.Lock()
					_ = clientConn.WriteMessage(websocket.TextMessage, pretty)
					clientWriteMu.Unlock()
				}

			case "response.audio.delta": //Azure Ver. | OpenAI Ver. response.output_audio.delta
				if !allowAudio {
					// barge-in 期間：丟掉舊 response 的 audio（避免 tail 音）
					continue
				}

				delta, _ := evt["delta"].(string)
				pcm, err := base64.StdEncoding.DecodeString(delta)
				if err != nil {
					log.Println("decode delta error:", err)
					continue
				}

				log.Printf("→ sending %d bytes of PCM to client\n", len(pcm))
				clientWriteMu.Lock()
				if framedBinary {
					err = clientConn.WriteMessage(websocket.BinaryMessage, framePCM(pcm))
				} else {
					err = clientConn.WriteMessage(websocket.BinaryMessage, pcm)
				}
				clientWriteMu.Unlock()
				if err != nil {
					log.Printf("failed to send PCM to client: %v\n", err)
				}

			case "response.done":
				log.Println("🟢 response.done")
				responseActive = false

				// 在某些 edge case，如果 speech_started 期間把 allowAudio 關掉，
				// response.done 後回到 idle 狀態，允許下一輪 response 轉發。
				if !inSpeech {
					allowAudio = true
				}

			default:
				// 初期你想觀察事件就留著；穩定後可註解掉避免洗版
				log.Println("openai event:", t)
			}
		}
	}()

	// ---- Client → OpenAI：binary audio 直接 append（不再做 idle commit）----
	for {
		msgType, data, err := clientConn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
				websocket.CloseNoStatusReceived,
			) {
				log.Println("client disconnected")
			} else {
				log.Println("client read error:", err)
			}
			cancel()
			return
		}

		switch msgType {
		case websocket.BinaryMessage:
			// 直接 append。OpenAI Realtime 會自動進行 cut-through
			// （當有新 input 時自動中斷 response，不需要手動 cancel）
			openaiWriter.SendAudio(openAIEvent{
				"type":  "input_audio_buffer.append",
				"audio": base64.StdEncoding.EncodeToString(data),
			})

		case websocket.TextMessage:
			// debug/控制命令（可選）
			cmd := string(data)
			switch cmd {
			case "clear":
				log.Println("→ cmd clear")
				openaiWriter.SendControl(openAIEvent{"type": "input_audio_buffer.clear"})

			case "cancel":
				log.Println("→ cmd response.cancel")
				openaiWriter.SendControl(openAIEvent{"type": "response.cancel"})
			case "force":
				// 可選：強制讓模型開始回（有時你想立即回不想等 VAD）
				log.Println("→ cmd response.create (force)")
				openaiWriter.SendControl(openAIEvent{
					"type":     "response.create",
					"response": openAIEvent{"output_modalities": []string{"audio"}},
				})

			default:
				log.Println("client text:", cmd)
			}
		}
	}
}

func dialOpenAIRealtime() (*websocket.Conn, error) {
	apiKey := os.Getenv("AZURE_OPENAI_API_KEY")
	h := http.Header{}
	// h.Set("Authorization", "Bearer "+apiKey) // OpenAI Ver.
	h.Add("api-key", apiKey) // Azure OpenAI Ver.

	conn, _, err := websocket.DefaultDialer.Dial(openAIRealtimeURL, h)
	return conn, err
}

// ---- 單一 Writer（含 ping）----
// gorilla/websocket：同一條連線只允許一個 goroutine 寫入，這個結構就是為了解決它
type WSWriter struct {
	conn      *websocket.Conn
	controlCh chan []byte
	audioCh   chan []byte
}

func NewWSWriter(ctx context.Context, conn *websocket.Conn) *WSWriter {
	w := &WSWriter{
		conn:      conn,
		controlCh: make(chan []byte),    // 不丟，保序
		audioCh:   make(chan []byte, 4), // ~80ms audio buffer
	}

	go w.loop(ctx)
	return w
}

func (w *WSWriter) loop(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		// 1️⃣ Control 優先
		case msg := <-w.controlCh:
			_ = w.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := w.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		// 2️⃣ Audio（可能被丟）
		case msg := <-w.audioCh:
			_ = w.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := w.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		// 3️⃣ Ping
		case <-ticker.C:
			_ = w.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := w.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (w *WSWriter) SendControl(v any) {
	b, _ := json.Marshal(v)
	w.controlCh <- b // block 是刻意的
}

func (w *WSWriter) SendAudio(v any) {
	b, _ := json.Marshal(v)

	select {
	case w.audioCh <- b:
		// 成功送進 buffer
	default:
		// buffer 滿了，丟掉最舊的
		<-w.audioCh
		w.audioCh <- b
	}
}

// clientConn 的 ping loop：注意要用同一把 clientWriteMu
func pingLoop(ctx context.Context, conn *websocket.Conn, mu *sync.Mutex) {
	t := time.NewTicker(pingPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			mu.Lock()
			err := conn.WriteMessage(websocket.PingMessage, nil)
			mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}
