package processor

import (
	"regexp"
	"strings"
)

// MaskSensitiveInfo 对日志内容中的敏感信息进行脱敏处理
func MaskSensitiveInfo(content string) string {
	// 替换敏感关键词
	sensitiveKeywords := map[string]string{
		"password": "[REDACTED]",
		"passwd":   "[REDACTED]",
		"token":    "[TOKEN]",
		"apikey":   "[API_KEY]",
		"username": "USER",
		"user":     "USER",
		"email":    "[EMAIL]",
		"phone":    "[PHONE]",
	}

	for k, v := range sensitiveKeywords {
		content = strings.ReplaceAll(content, k, v)
	}

	// 使用正则匹配并脱敏邮箱地址
	emailRegex := regexp.MustCompile(`[\w\.-]+@[\w\.-]+\.\w+`)
	content = emailRegex.ReplaceAllString(content, "[EMAIL]")

	// 匹配手机号码
	phoneRegex := regexp.MustCompile(`\b1[3-9]\d{9}\b`)
	content = phoneRegex.ReplaceAllString(content, "[PHONE]")

	// 匹配疑似Token（长度较长的字母数字组合）
	tokenRegex := regexp.MustCompile(`(?i)(token\s*[:=]\s*)[\w\-]{16,}`)
	content = tokenRegex.ReplaceAllString(content, "${1}[TOKEN]")

	return content
}
