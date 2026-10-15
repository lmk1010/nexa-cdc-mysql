// Package alert —— 出问题时推一条文本消息到 webhook（钉钉/企业微信机器人格式兼容）。
// 同一个 key 10 分钟内只发一次，避免刷屏。webhook 没配就只打日志。
package alert

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type Notifier struct {
	url     string
	keyword string
	mu      sync.Mutex
	last    map[string]time.Time
	client  *http.Client
}

func New(url, keyword string) *Notifier {
	return &Notifier{url: url, keyword: keyword, last: map[string]time.Time{}, client: &http.Client{Timeout: 5 * time.Second}}
}

// Notify 异步发送；key 用来去重限流。
func (n *Notifier) Notify(key, text string) {
	if n == nil {
		return
	}
	n.mu.Lock()
	if t, ok := n.last[key]; ok && time.Since(t) < 10*time.Minute {
		n.mu.Unlock()
		return
	}
	n.last[key] = time.Now()
	n.mu.Unlock()
	log.Printf("[alert] %s", text)
	if n.url == "" {
		return
	}
	content := text
	if n.keyword != "" {
		content = n.keyword + " " + text
	}
	body, _ := json.Marshal(map[string]any{
		"msgtype": "text",
		"text":    map[string]string{"content": content},
	})
	go func() {
		resp, err := n.client.Post(n.url, "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("[alert] webhook failed: %v", err)
			return
		}
		_ = resp.Body.Close()
	}()
}
