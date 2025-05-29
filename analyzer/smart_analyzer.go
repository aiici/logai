package analyzer

import (
	"crypto/md5"
	"fmt"
	"log-ai-analyzer/collector"
	"log-ai-analyzer/esclient"
	"sort"
	"strings"
	"sync"
	"time"
)

// SmartAnalyzer 智能分析器，用于事件关联、去重和智能分组
type SmartAnalyzer struct {
	eventCache    map[string]*CachedEvent // 事件缓存
	relatedEvents map[string][]string     // 相关事件映射
	mutex         sync.RWMutex            // 读写锁
	cacheTTL      time.Duration           // 缓存过期时间
}

// CachedEvent 缓存的事件信息
type CachedEvent struct {
	Event     *collector.LogEvent
	FirstSeen time.Time
	LastSeen  time.Time
	Count     int
	Signature string // 事件签名，用于去重
}

// NewSmartAnalyzer 创建新的智能分析器
func NewSmartAnalyzer(cacheTTL time.Duration) *SmartAnalyzer {
	return &SmartAnalyzer{
		eventCache:    make(map[string]*CachedEvent),
		relatedEvents: make(map[string][]string),
		mutex:         sync.RWMutex{},
		cacheTTL:      cacheTTL,
	}
}

// AnalyzeEvent 分析事件，返回是否为新事件和相关事件
func (sa *SmartAnalyzer) AnalyzeEvent(event *collector.LogEvent) (isNew bool, relatedEventIDs []string, enhancedEvent *collector.LogEvent) {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	// 生成事件签名
	signature := sa.generateEventSignature(event)

	// 检查是否为重复事件
	if cachedEvent, exists := sa.eventCache[signature]; exists {
		// 更新缓存事件
		cachedEvent.LastSeen = time.Now()
		cachedEvent.Count++

		// 返回相关事件
		return false, sa.relatedEvents[event.EventID], event
	}

	// 新事件，添加到缓存
	cachedEvent := &CachedEvent{
		Event:     event,
		FirstSeen: time.Now(),
		LastSeen:  time.Now(),
		Count:     1,
		Signature: signature,
	}
	sa.eventCache[signature] = cachedEvent

	// 查找相关事件
	relatedIDs := sa.findRelatedEvents(event)
	sa.relatedEvents[event.EventID] = relatedIDs

	// 增强事件信息
	enhanced := sa.enhanceEvent(event, relatedIDs)

	return true, relatedIDs, enhanced
}

// generateEventSignature 生成事件签名用于去重
func (sa *SmartAnalyzer) generateEventSignature(event *collector.LogEvent) string {
	// 使用关键信息生成签名
	key := fmt.Sprintf("%s|%s|%d|%s",
		event.FilePath,
		sa.normalizeContent(event.RawText),
		event.SeverityScore,
		strings.Join(event.Tags, ","),
	)

	hash := md5.Sum([]byte(key))
	return fmt.Sprintf("%x", hash)
}

// normalizeContent 标准化内容，去除时间戳等变化的部分
func (sa *SmartAnalyzer) normalizeContent(content string) string {
	// 移除常见的变化部分
	normalized := content

	// 移除时间戳
	timestampPatterns := []string{
		`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`,
		`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
		`\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}`,
	}

	for _, pattern := range timestampPatterns {
		normalized = strings.ReplaceAll(normalized, pattern, "[TIMESTAMP]")
	}

	// 移除数字ID
	normalized = strings.ReplaceAll(normalized, `\b\d{6,}\b`, "[ID]")

	// 移除内存地址
	normalized = strings.ReplaceAll(normalized, `0x[a-fA-F0-9]+`, "[ADDR]")

	return normalized
}

// findRelatedEvents 查找相关事件
func (sa *SmartAnalyzer) findRelatedEvents(event *collector.LogEvent) []string {
	var relatedIDs []string

	// 基于时间窗口查找相关事件
	timeWindow := 5 * time.Minute
	currentTime := time.Now()

	for _, cachedEvent := range sa.eventCache {
		if cachedEvent.Event.EventID == event.EventID {
			continue
		}

		// 时间窗口检查
		if currentTime.Sub(cachedEvent.LastSeen) > timeWindow {
			continue
		}

		// 相关性检查
		if sa.areEventsRelated(event, cachedEvent.Event) {
			relatedIDs = append(relatedIDs, cachedEvent.Event.EventID)
		}
	}

	return relatedIDs
}

// areEventsRelated 判断两个事件是否相关
func (sa *SmartAnalyzer) areEventsRelated(event1, event2 *collector.LogEvent) bool {
	// 同一文件
	if event1.FilePath == event2.FilePath {
		return true
	}

	// 相同的错误类型和组件
	if len(event1.Tags) > 0 && len(event2.Tags) > 0 {
		commonTags := sa.getCommonTags(event1.Tags, event2.Tags)
		if len(commonTags) > 0 {
			return true
		}
	}

	// 相似的内容
	if sa.calculateContentSimilarity(event1.RawText, event2.RawText) > 0.7 {
		return true
	}

	return false
}

// getCommonTags 获取共同标签
func (sa *SmartAnalyzer) getCommonTags(tags1, tags2 []string) []string {
	tagSet := make(map[string]bool)
	for _, tag := range tags1 {
		tagSet[tag] = true
	}

	var common []string
	for _, tag := range tags2 {
		if tagSet[tag] {
			common = append(common, tag)
		}
	}

	return common
}

// calculateContentSimilarity 计算内容相似度
func (sa *SmartAnalyzer) calculateContentSimilarity(content1, content2 string) float64 {
	// 简单的词汇相似度计算
	words1 := strings.Fields(strings.ToLower(content1))
	words2 := strings.Fields(strings.ToLower(content2))

	if len(words1) == 0 || len(words2) == 0 {
		return 0
	}

	wordSet1 := make(map[string]bool)
	for _, word := range words1 {
		wordSet1[word] = true
	}

	commonWords := 0
	for _, word := range words2 {
		if wordSet1[word] {
			commonWords++
		}
	}

	totalWords := len(words1) + len(words2) - commonWords
	if totalWords == 0 {
		return 1.0
	}

	return float64(commonWords*2) / float64(totalWords)
}

// enhanceEvent 增强事件信息
func (sa *SmartAnalyzer) enhanceEvent(event *collector.LogEvent, relatedEventIDs []string) *collector.LogEvent {
	// 创建增强的事件副本
	enhanced := *event

	// 添加相关事件信息到上下文
	if len(relatedEventIDs) > 0 {
		relatedInfo := fmt.Sprintf("Related Events: %s", strings.Join(relatedEventIDs, ", "))
		enhanced.ContextLines = append(enhanced.ContextLines, relatedInfo)
	}

	return &enhanced
}

// CleanupExpiredEvents 清理过期事件
func (sa *SmartAnalyzer) CleanupExpiredEvents() {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	currentTime := time.Now()
	for signature, cachedEvent := range sa.eventCache {
		if currentTime.Sub(cachedEvent.LastSeen) > sa.cacheTTL {
			delete(sa.eventCache, signature)
			delete(sa.relatedEvents, cachedEvent.Event.EventID)
		}
	}
}

// GetEventStatistics 获取事件统计信息
func (sa *SmartAnalyzer) GetEventStatistics() map[string]interface{} {
	sa.mutex.RLock()
	defer sa.mutex.RUnlock()

	stats := make(map[string]interface{})
	stats["total_cached_events"] = len(sa.eventCache)
	stats["total_related_mappings"] = len(sa.relatedEvents)

	// 按严重性分组统计
	severityStats := make(map[int]int)
	for _, cachedEvent := range sa.eventCache {
		severityStats[cachedEvent.Event.SeverityScore]++
	}
	stats["severity_distribution"] = severityStats

	// 最频繁的事件
	type EventFreq struct {
		Signature string
		Count     int
		Event     *collector.LogEvent
	}

	var frequencies []EventFreq
	for signature, cachedEvent := range sa.eventCache {
		frequencies = append(frequencies, EventFreq{
			Signature: signature,
			Count:     cachedEvent.Count,
			Event:     cachedEvent.Event,
		})
	}

	// 按频率排序
	sort.Slice(frequencies, func(i, j int) bool {
		return frequencies[i].Count > frequencies[j].Count
	})

	// 取前10个最频繁的事件
	topEvents := make([]map[string]interface{}, 0)
	for i, freq := range frequencies {
		if i >= 10 {
			break
		}
		topEvents = append(topEvents, map[string]interface{}{
			"event_id": freq.Event.EventID,
			"count":    freq.Count,
			"content":  freq.Event.RawText[:min(100, len(freq.Event.RawText))],
		})
	}
	stats["top_frequent_events"] = topEvents

	return stats
}

// ConvertToESEvent 将增强的LogEvent转换为ES事件
func (sa *SmartAnalyzer) ConvertToESEvent(event *collector.LogEvent, aiResult string, relatedEventIDs []string) esclient.LogEvent {
	timestamp, _ := time.Parse(time.RFC3339, event.Timestamp)

	return esclient.LogEvent{
		EventID:       event.EventID,
		Timestamp:     timestamp,
		Host:          event.Host,
		Tags:          event.Tags,
		Content:       event.RawText,
		RawLines:      event.RawLines,
		SeverityScore: event.SeverityScore,
		AiResult:      aiResult,
		FilePath:      event.FilePath,
		LineNumber:    event.LineNumber,
		ContextLines:  event.ContextLines,
		IsCellTrace:   event.IsCellTrace,
		RelatedEvents: relatedEventIDs,
		ProcessedAt:   time.Now(),
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
