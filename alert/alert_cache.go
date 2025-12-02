package alert

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"log-ai-analyzer/collector"
	"log-ai-analyzer/metrics"
)

type AggregatedAlert struct {
	EventID      string
	Host         string
	Severity     int
	Count        int
	LastAlertAt  time.Time
	FirstAlertAt time.Time
	Content      string
	AiResult     string
	IsCellTrace  bool     // 标识是否为Cell Trace异常
	FilePath     string   // 文件路径
	ContextLines []string // 上下文行
	TotalScore   int      // 累计严重性分数
}

type AlertCache struct {
	cache map[string]*AggregatedAlert
	ttl   time.Duration
	mu    sync.Mutex
}

func NewAlertCache(ttl time.Duration) *AlertCache {
	return &AlertCache{
		cache: make(map[string]*AggregatedAlert),
		ttl:   ttl,
	}
}

// generateAlertKey 生成告警的唯一键
func generateAlertKey(event collector.LogEvent) string {
	// 总是基于内容生成稳定的键，确保一致性
	contentHash := getContentHash(event.RawText)
	
	// 结合主机名、文件路径和内容哈希生成键
	baseKey := fmt.Sprintf("%s-%s-%s", event.Host, getFileName(event.FilePath), contentHash)
	
	// 如果是Cell Trace异常，添加特殊前缀
	if event.IsCellTrace {
		return fmt.Sprintf("cell-trace-%s", baseKey)
	}
	
	return baseKey
}

// getContentHash 获取内容的哈希值
func getContentHash(content string) string {
	// 使用与collector中相同的标准化方法
	normalized := normalizeContent(content)
	
	// 计算MD5哈希
	hash := md5.Sum([]byte(normalized))
	return hex.EncodeToString(hash[:])
}

// normalizeContent 标准化内容，移除变化的部分
func normalizeContent(content string) string {
	// 移除时间戳
	re := regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}[\s\d.:]*|\w{3} \d{1,2} \d{2}:\d{2}:\d{2}`)
	content = re.ReplaceAllString(content, "")
	
	// 移除可能变化的数字（如PID、端口号等）
	re = regexp.MustCompile(`\b\d+\b`)
	content = re.ReplaceAllString(content, "NUMBER")
	
	// 转换为小写以提高一致性
	content = strings.ToLower(content)
	
	// 移除多余的空白字符
	content = regexp.MustCompile(`\s+`).ReplaceAllString(content, " ")
	
	return strings.TrimSpace(content)
}

// getFileName 从完整路径中提取文件名
func getFileName(path string) string {
	parts := strings.Split(path, "/")
	filename := parts[len(parts)-1]
	parts = strings.Split(filename, "\\")
	return parts[len(parts)-1]
}

// 告警合并策略
func (ac *AlertCache) AddOrUpdate(event collector.LogEvent, aiResult string) (send bool, alert AggregatedAlert) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// 生成更智能的告警键
	key := generateAlertKey(event)

	now := time.Now()
	if agg, ok := ac.cache[key]; ok {
		// 更新现有告警
		agg.Count++
		agg.LastAlertAt = now
		agg.Severity = max(agg.Severity, event.SeverityScore)
		agg.TotalScore += event.SeverityScore
		agg.Content = event.RawText // 使用最新的内容
		agg.AiResult = aiResult

		// 合并上下文行（去重）
		if len(event.ContextLines) > 0 {
			agg.ContextLines = mergeContextLines(agg.ContextLines, event.ContextLines)
		}

		// 更智能的告警合并策略
		// 对于高严重性问题(严重性>=8)，采用更积极的告警策略
		if event.SeverityScore >= 8 {
			// 高严重性问题：前3次立即发送，之后每5分钟发送一次
			if agg.Count <= 3 {
				send = true
			} else {
				// 检查是否距离上次发送已经超过5分钟
				send = now.Sub(agg.LastAlertAt) >= 5*time.Minute
			}
		} else if event.SeverityScore >= 5 {
			// 中等严重性问题：前2次立即发送，之后每10分钟发送一次
			if agg.Count <= 2 {
				send = true
			} else {
				// 检查是否距离上次发送已经超过10分钟
				send = now.Sub(agg.LastAlertAt) >= 10*time.Minute
			}
		} else {
			// 低严重性问题：每10次或每30分钟发送一次
			send = (agg.Count%10 == 0) || (now.Sub(agg.LastAlertAt) >= 30*time.Minute)
		}
		
		// 如果是新的Cell Trace异常，即使已有相同类型也要发送
		if event.IsCellTrace && !agg.IsCellTrace {
			agg.IsCellTrace = true
			send = true
		}

		alert = *agg
		
		// 更新合并告警计数
		if send {
			metrics.AlertMergedCount.Inc()
		}
		
		// 如果是Cell Trace异常，更新相关指标
		if event.IsCellTrace {
			metrics.CellTraceErrorCount.Inc()
			metrics.CellTraceErrorSeverity.Observe(float64(event.SeverityScore))
		}
	} else {
		// 在创建新告警之前，检查是否存在相似度很高的事件（基于90%的阈值）
		var similarKey string
		for k, agg := range ac.cache {
			// 检查主机名和文件路径是否匹配
			if agg.Host == event.Host && agg.FilePath == event.FilePath {
				// 创建临时事件用于比较
				tempEvent := collector.LogEvent{
					RawText: agg.Content,
					Host:    agg.Host,
				}
				
				// 检查相似度
				if isSimilarEnough(event, tempEvent) {
					similarKey = k
					break
				}
			}
		}

		// 如果找到了相似度很高的事件，则合并到该事件中
		if similarKey != "" {
			agg := ac.cache[similarKey]
			// 更新现有告警
			agg.Count++
			agg.LastAlertAt = now
			agg.Severity = max(agg.Severity, event.SeverityScore)
			agg.TotalScore += event.SeverityScore
			agg.Content = event.RawText // 使用最新的内容
			agg.AiResult = aiResult

			// 合并上下文行（去重）
			if len(event.ContextLines) > 0 {
				agg.ContextLines = mergeContextLines(agg.ContextLines, event.ContextLines)
			}

			// 更智能的告警合并策略
			// 对于高严重性问题(严重性>=8)，采用更积极的告警策略
			if event.SeverityScore >= 8 {
				// 高严重性问题：前3次立即发送，之后每5分钟发送一次
				if agg.Count <= 3 {
					send = true
				} else {
					// 检查是否距离上次发送已经超过5分钟
					send = now.Sub(agg.LastAlertAt) >= 5*time.Minute
				}
			} else if event.SeverityScore >= 5 {
				// 中等严重性问题：前2次立即发送，之后每10分钟发送一次
				if agg.Count <= 2 {
					send = true
				} else {
					// 检查是否距离上次发送已经超过10分钟
					send = now.Sub(agg.LastAlertAt) >= 10*time.Minute
				}
			} else {
				// 低严重性问题：每10次或每30分钟发送一次
				send = (agg.Count%10 == 0) || (now.Sub(agg.LastAlertAt) >= 30*time.Minute)
			}
			
			// 如果是新的Cell Trace异常，即使已有相同类型也要发送
			if event.IsCellTrace && !agg.IsCellTrace {
				agg.IsCellTrace = true
				send = true
			}

			alert = *agg
			
			// 更新合并告警计数
			if send {
				metrics.AlertMergedCount.Inc()
			}
			
			// 如果是Cell Trace异常，更新相关指标
			if event.IsCellTrace {
				metrics.CellTraceErrorCount.Inc()
				metrics.CellTraceErrorSeverity.Observe(float64(event.SeverityScore))
			}
			return
		}

		// 创建新告警
		ac.cache[key] = &AggregatedAlert{
			EventID:      event.EventID,
			Host:         event.Host,
			Severity:     event.SeverityScore,
			Count:        1,
			LastAlertAt:  now,
			FirstAlertAt: now,
			Content:      event.RawText,
			AiResult:     aiResult,
			IsCellTrace:  event.IsCellTrace,
			FilePath:     event.FilePath,
			ContextLines: event.ContextLines,
			TotalScore:   event.SeverityScore,
		}

		// 新告警的发送策略
		// 根据严重性评分决定是否立即发送
		if event.SeverityScore >= 8 {
			// 高严重性异常立即发送
			send = true
		} else if event.SeverityScore >= 5 {
			// 中等严重性异常立即发送
			send = true
		} else {
			// 低严重性异常延迟发送，但如果是Cell Trace则发送
			send = event.IsCellTrace
		}

		alert = *ac.cache[key]
		
		// 如果是Cell Trace异常，更新相关指标
		if event.IsCellTrace {
			metrics.CellTraceErrorCount.Inc()
			metrics.CellTraceErrorSeverity.Observe(float64(event.SeverityScore))
		}
	}
	return
}

// 合并上下文行，去重并保持顺序
func mergeContextLines(existing, new []string) []string {
	if len(new) == 0 {
		return existing
	}
	if len(existing) == 0 {
		return new
	}

	// 使用map去重
	seen := make(map[string]bool)
	var result []string

	// 先添加现有的行
	for _, line := range existing {
		if !seen[line] {
			seen[line] = true
			result = append(result, line)
		}
	}

	// 再添加新的行
	for _, line := range new {
		if !seen[line] {
			seen[line] = true
			result = append(result, line)
		}
	}

	// 限制上下文行数，避免过多
	if len(result) > 20 {
		return result[:20]
	}
	return result
}

// calculateSimilarity 计算两个字符串的相似度（0-100）
func calculateSimilarity(s1, s2 string) float64 {
	// 如果两个字符串完全相同，直接返回100
	if s1 == s2 {
		return 100.0
	}

	// 转换为小写进行比较
	s1 = strings.ToLower(s1)
	s2 = strings.ToLower(s2)

	// 移除空白字符
	s1 = removeWhitespace(s1)
	s2 = removeWhitespace(s2)

	// 如果其中一个为空，则相似度为0
	if len(s1) == 0 || len(s2) == 0 {
		return 0.0
	}

	// 使用编辑距离算法计算相似度
	distance := levenshteinDistance(s1, s2)
	maxLen := max(len(s1), len(s2))
	
	if maxLen == 0 {
		return 100.0
	}
	
	// 计算相似度百分比
	similarity := (1.0 - float64(distance)/float64(maxLen)) * 100.0
	return similarity
}

// removeWhitespace 移除字符串中的所有空白字符
func removeWhitespace(s string) string {
	var result strings.Builder
	for _, r := range s {
		if !unicode.IsSpace(r) {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// levenshteinDistance 计算两个字符串的编辑距离
func levenshteinDistance(s1, s2 string) int {
	// 创建二维数组
	m, n := len(s1), len(s2)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	// 初始化边界条件
	for i := 0; i <= m; i++ {
		dp[i][0] = i
	}
	for j := 0; j <= n; j++ {
		dp[0][j] = j
	}

	// 动态规划填表
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if s1[i-1] == s2[j-1] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]) + 1
			}
		}
	}

	return dp[m][n]
}

// min 返回三个整数中的最小值
func min(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= a && b <= c {
		return b
	}
	return c
}

// max 返回两个整数中的最大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// isSimilarEnough 检查两个日志事件是否足够相似（基于90%的阈值）
func isSimilarEnough(event1, event2 collector.LogEvent) bool {
	// 首先检查基本属性是否一致
	if event1.Host != event2.Host {
		return false
	}

	// 计算内容相似度
	similarity := calculateSimilarity(event1.RawText, event2.RawText)
	return similarity >= 90.0
}

func (ac *AlertCache) Cleanup() {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	now := time.Now()
	for k, v := range ac.cache {
		if now.Sub(v.LastAlertAt) > ac.ttl {
			delete(ac.cache, k)
		}
	}
}
