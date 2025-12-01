package processor

import (
	"regexp"
	_ "strings"
)

// MaskSensitiveInfo 对日志内容中的敏感信息进行脱敏处理
func MaskSensitiveInfo(content string) string {
	// 替换敏感关键词
	sensitiveKeywords := map[string]string{
		"password":    "[REDACTED]",
		"passwd":      "[REDACTED]",
		"token":       "[TOKEN]",
		"apikey":      "[API_KEY]",
		"api_key":     "[API_KEY]",
		"username":    "USER",
		"user":        "USER",
		"email":       "[EMAIL]",
		"phone":       "[PHONE]",
		"telephone":   "[PHONE]",
		"mobile":      "[PHONE]",
		"credit_card": "[CREDIT_CARD]",
		"creditcard":  "[CREDIT_CARD]",
		"ssn":         "[SSN]",
		"social":      "[SSN]",
		"address":     "[ADDRESS]",
		"ip":          "[IP]",
		"ip_address":  "[IP]",
		"mac":         "[MAC]",
		"mac_address": "[MAC]",
		"key":         "[KEY]",
		"secret":      "[SECRET]",
		"certificate": "[CERTIFICATE]",
		"cert":        "[CERTIFICATE]",
		"private_key": "[PRIVATE_KEY]",
		"public_key":  "[PUBLIC_KEY]",
	}

	for k, v := range sensitiveKeywords {
		// 使用正则表达式匹配关键词，支持不同的分隔符
		pattern := `(?i)(` + k + `)(\s*[:=]\s*["']?)([^"'\s]+)(["']?)`
		re := regexp.MustCompile(pattern)
		content = re.ReplaceAllString(content, `${1}${2}`+v+`${4}`)
	}

	// 使用正则匹配并脱敏邮箱地址
	emailRegex := regexp.MustCompile(`[\w\.-]+@[\w\.-]+\.\w+`)
	content = emailRegex.ReplaceAllString(content, "[EMAIL]")

	// 匹配手机号码 (中国手机号)
	phoneRegex := regexp.MustCompile(`\b1[3-9]\d{9}\b`)
	content = phoneRegex.ReplaceAllString(content, "[PHONE]")

	// 匹配IP地址
	ipRegex := regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)
	content = ipRegex.ReplaceAllString(content, "[IP]")

	// 匹配MAC地址
	macRegex := regexp.MustCompile(`([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})`)
	content = macRegex.ReplaceAllString(content, "[MAC]")

	// 匹配身份证号 (18位或15位)
	idRegex := regexp.MustCompile(`\b[1-9]\d{5}(18|19|20)\d{2}((0[1-9])|(1[0-2]))(([0-2][1-9])|10|20|30|31)\d{3}[0-9Xx]\b|\b[1-9]\d{5}\d{2}((0[1-9])|(1[0-2]))(([0-2][1-9])|10|20|30|31)\d{3}\b`)
	content = idRegex.ReplaceAllString(content, "[ID_NUMBER]")

	// 匹配信用卡号 (16位数字，可能有分隔符)
	cardRegex := regexp.MustCompile(`\b(?:\d{4}[-\s]?){3}\d{4}\b`)
	content = cardRegex.ReplaceAllString(content, "[CREDIT_CARD]")

	// 匹配疑似Token（长度较长的字母数字组合）
	tokenRegex := regexp.MustCompile(`(?i)(token\s*[:=]\s*)[\w\-]{16,}`)
	content = tokenRegex.ReplaceAllString(content, "${1}[TOKEN]")

	// 匹配JWT Token
	jwtRegex := regexp.MustCompile(`eyJ[A-Za-z0-9-_]*\.[A-Za-z0-9-_]*\.[A-Za-z0-9-_]*`)
	content = jwtRegex.ReplaceAllString(content, "[JWT_TOKEN]")

	// 匹配URL中的敏感参数
	urlParamRegex := regexp.MustCompile(`([&?](password|token|key|secret)=)([^&]*)`)
	content = urlParamRegex.ReplaceAllString(content, `${1}[REDACTED]`)

	return content
}
