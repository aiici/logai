package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
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

// SendWeChat 发送告警到企业微信
func SendWeChat(webhook, content, aiResult string) error {
	msg := WeChatMessage{
		MsgType: "markdown",
		Markdown: Markdown{
			Content: formatWeChatMessage(content, aiResult),
		},
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	resp, err := http.Post(webhook, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("发送HTTP请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %w", err)
	}

	var r WeChatResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("解析响应失败: %w", err)
	}

	if r.ErrCode != 0 {
		return fmt.Errorf("企业微信返回错误: %s (错误码: %d)", r.ErrMsg, r.ErrCode)
	}
	return nil
}

// formatWeChatMessage 格式化企业微信告警消息
func formatWeChatMessage(content, aiResult string) string {
	return fmt.Sprintf(
		"### 🚨 **日志异常告警**\n"+
			"> 时间: %s\n"+
			"**📜 日志内容:**\n``\n%s\n``\n"+
			"**🤖 AI 分析:**\n\n%s\n",
		time.Now().Format("2006-01-02 15:04:05"),
		content, aiResult,
	)
}

// SendDingTalk 发送到钉钉机器人（预留扩展）
func SendDingTalk(webhook, content, aiResult string) error {
	// TODO: 实现钉钉机器人发送逻辑
	return nil
}

// SendFeishu 发送到飞书机器人（预留扩展）
func SendFeishu(webhook, content, aiResult string) error {
	// TODO: 实现飞书机器人发送逻辑
	return nil
}
