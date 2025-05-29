package alert

import (
	"fmt"
	"sync"
	"time"

	"log-ai-analyzer/collector"
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

// 告警合并策略
func (ac *AlertCache) AddOrUpdate(event collector.LogEvent, aiResult string) (send bool, alert AggregatedAlert) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// 对于Cell Trace异常，使用更精确的key
	var key string
	if event.IsCellTrace && event.EventID != "" {
		// Cell Trace异常使用EventID作为主要标识
		key = fmt.Sprintf("cell-trace-%s-%s", event.Host, event.EventID)
	} else if event.EventID != "" {
		// 有EventID的普通异常
		key = fmt.Sprintf("%s-%s", event.Host, event.EventID)
	} else {
		// 没有EventID的异常，使用文件路径和行号
		key = fmt.Sprintf("%s-%s-%d", event.Host, event.FilePath, event.LineNumber)
	}

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

		// Cell Trace异常或高严重性异常的特殊处理
		if event.IsCellTrace || event.SeverityScore >= 8 {
			// 如果是第一次出现Cell Trace或高严重性异常，立即发送
			if !agg.IsCellTrace && event.IsCellTrace {
				agg.IsCellTrace = true
				send = true
			} else if agg.Count <= 3 { // 前3次高严重性异常都发送
				send = true
			} else {
				// 后续按时间间隔发送（每5分钟一次）
				send = now.Sub(agg.LastAlertAt) >= 5*time.Minute
			}
		} else {
			// 普通异常的合并策略：累计到一定数量或时间间隔后发送
			send = (agg.Count%10 == 0) || (now.Sub(agg.LastAlertAt) >= 10*time.Minute)
		}

		alert = *agg
	} else {
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
		if event.IsCellTrace || event.SeverityScore >= 8 {
			// Cell Trace异常和高严重性异常立即发送
			send = true
		} else if event.SeverityScore >= 5 {
			// 中等严重性异常也立即发送
			send = true
		} else {
			// 低严重性异常延迟发送
			send = false
		}

		alert = *ac.cache[key]
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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
