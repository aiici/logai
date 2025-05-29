package ai

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"log-ai-analyzer/config"
)

func Analyze(cfg *config.Config, content string) (string, error) {
	// 是否启用 AI 分析
	if !config.IsTrue(cfg.AIEnable) {
		return " ", nil
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
	}

	body, _ := json.Marshal(data)
	req, _ := http.NewRequest("POST", cfg.AIAPIURL, bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+cfg.AIAPIKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

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
				// 检查是否有 choices 字段,空接口断言逻辑
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
