package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type WeChatMessage struct {
	MsgType  string   `json:"msgtype"`
	Markdown Markdown `json:"markdown"`
}

type Markdown struct {
	Content string `json:"content"`
}

type WeChatResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

func SendWeChat(webhook, content, aiResult string) error {
	msg := WeChatMessage{
		MsgType: "markdown",
		Markdown: Markdown{
			Content: fmt.Sprintf(
				"### 🚨 **日志异常告警** 🚨\n**📜 日志内容:**\n```\n%s\n```\n**🤖 AI 分析:**\n```\n%s\n```",
				content, aiResult,
			),
		},
	}
	payload, _ := json.Marshal(msg)

	resp, err := http.Post(webhook, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var r WeChatResponse
	if json.Unmarshal(body, &r) == nil && r.ErrCode != 0 {
		return fmt.Errorf("企业微信返回错误: %s", r.ErrMsg)
	}
	return nil
}
