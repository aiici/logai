package ai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"log-ai-analyzer/config"
)

// AnalyzeResult AI分析结果
type AnalyzeResult struct {
	Content string
	Error   error
}

// Analyze 对日志内容进行AI分析
func Analyze(cfg *config.Config, content string) (string, error) {
	if strings.ToLower(cfg.AIEnable) != "true" {
		return "AI 分析未启用", nil
	}

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resultChan := make(chan AnalyzeResult, 1)

	// 在goroutine中执行AI分析，支持超时
	go func() {
		result, err := performAIAnalysis(cfg, content)
		resultChan <- AnalyzeResult{Content: result, Error: err}
	}()

	select {
	case <-ctx.Done():
		return "", fmt.Errorf("AI分析超时")
	case res := <-resultChan:
		return res.Content, res.Error
	}
}

// performAIAnalysis 执行实际的AI分析请求
func performAIAnalysis(cfg *config.Config, content string) (string, error) {
	// 重试机制，最多尝试3次
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			// 重试前等待一段时间
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		systemPrompt := `
你是一位资深的 Linux 系统工程师，擅长分析系统日志和故障排查。
请你根据以下日志：
1. 识别关键错误和潜在问题，按重要程度排序。
2. 分析错误原因，并提供详细的技术解释。
3. 给出专业的修复建议，包括具体的 Linux 命令、配置修改方案或优化建议。
4. 如果是严重问题，请说明可能的影响和紧急处理措施。
请按照专业系统工程师的方式进行分析，并输出清晰的报告格式。
`

		data := map[string]interface{}{
			"model": cfg.AIModel,
			"messages": []map[string]string{
				{"role": "system", "content": systemPrompt},
				{"role": "user", "content": content},
			},
			"stream": true,
			"temperature": 0.7, // 增加温度参数以获得更好的创造性
		}

		body, err := json.Marshal(data)
		if err != nil {
			lastErr = fmt.Errorf("序列化请求数据失败: %w", err)
			continue
		}

		req, err := http.NewRequest("POST", cfg.AIAPIURL, bytes.NewBuffer(body))
		if err != nil {
			lastErr = fmt.Errorf("创建HTTP请求失败: %w", err)
			continue
		}
		req.Header.Set("Authorization", "Bearer "+cfg.AIAPIKey)
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{
			Timeout: 30 * time.Second, // 设置HTTP客户端超时
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("发送HTTP请求失败: %w", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("AI服务返回错误状态码: %d", resp.StatusCode)
			continue
		}

		result := []string{}
		reader := bufio.NewReader(resp.Body)

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			// 解析 JSON 数据
			if strings.HasPrefix(line, "data: ") {
				var payload map[string]interface{}
				if err := json.Unmarshal([]byte(line[6:]), &payload); err == nil {
					// 检查是否有 choices 字段
					choices, ok := payload["choices"]
					if !ok || choices == nil {
						continue
					}

					choicesArray, ok := choices.([]interface{})
					if !ok || len(choicesArray) == 0 {
						continue
					}

					choice, ok := choicesArray[0].(map[string]interface{})
					if !ok {
						continue
					}

					delta, ok := choice["delta"].(map[string]interface{})
					if !ok {
						continue
					}

					if content, ok := delta["content"].(string); ok {
						result = append(result, content)
					}
				}
			}
		}

		return strings.Join(result, ""), nil
	}

	return "", lastErr
}