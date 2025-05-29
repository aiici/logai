package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"log-ai-analyzer/ai"
	"log-ai-analyzer/alert"
	"log-ai-analyzer/analyzer"
	"log-ai-analyzer/collector"
	"log-ai-analyzer/config"
	"log-ai-analyzer/esclient"
	"log-ai-analyzer/metrics"
	"log-ai-analyzer/processor"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 0. 帮助文档
	if len(os.Args) > 1 {
		arg := os.Args[1]
		if arg == "-h" || arg == "--help" {
			appName := getAppName()
			printHelp(appName)
			os.Exit(0)
		}
	}

	// 1. 加载配置
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 2. 初始化ES客户端
	var esClient *esclient.ESClient
	if config.IsTrue(cfg.ESEnable) {
		esClient, err = esclient.NewESClient(cfg.ESNodes, cfg.ESIndex)
		if err != nil {
			log.Fatalf("初始化ES客户端失败: %v", err)
		}
		log.Println("✅ ES客户端初始化成功")
	} else {
		log.Println("⏭️ ES功能未启用，跳过初始化ES客户端")
	}

	// 3. 初始化告警缓存
	var alertCache *alert.AlertCache
	if config.IsTrue(cfg.AlertEnable) {
		alertCache = alert.NewAlertCache(cfg.AlertTTL)
		log.Println("✅ 告警缓存初始化成功")
	} else {
		log.Println("⏭️ 告警功能未启用，跳过初始化告警缓存")
	}

	// 4. 初始化智能分析器
	smartAnalyzer := analyzer.NewSmartAnalyzer(30 * time.Minute) // 30分钟缓存TTL
	if !config.IsTrue(cfg.AIEnable) {
		log.Println("⏭️ AI分析功能未启用，跳过AI分析")
	}
	// 5. 设置优雅退出
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 处理退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("收到退出信号，开始优雅退出...")
		cancel()
	}()

	// 5. 启动日志采集和处理
	// 创建工作池
	workerCount := 10
	if cfg.MaxWorkers > 0 {
		workerCount = cfg.MaxWorkers
	}

	// 创建事件处理通道
	eventChan := make(chan *collector.LogEvent, 100)

	// 启动工作池
	for i := 0; i < workerCount; i++ {
		go func() {
			for event := range eventChan {
				// 1. 数据脱敏
				event.RawText = processor.MaskSensitiveInfo(event.RawText)

				// 2. 智能分析和去重
				isNew, relatedEventIDs, enhancedEvent := smartAnalyzer.AnalyzeEvent(event)

				// 如果是重复事件，跳过处理（可选择性记录）
				if !isNew {
					log.Printf("检测到重复事件 [EventID: %s]，跳过处理", event.EventID)
					continue
				}

				// 3. AI分析
				start := time.Now()
				aiResult, err := ai.Analyze(cfg, enhancedEvent.RawText)
				metrics.AIAnalysisDuration.Observe(time.Since(start).Seconds())

				if err != nil {
					log.Printf("AI分析失败 [EventID: %s]: %v", enhancedEvent.EventID, err)
					metrics.AIAnalysisErrorCount.Inc()
					continue
				}

				// 4. 智能字段提取
				errorType := extractErrorType(enhancedEvent.RawText, enhancedEvent.Tags)
				component := extractComponent(enhancedEvent.FilePath, enhancedEvent.RawText)
				stackTrace := extractStackTrace(enhancedEvent.RawLines)
				suggestion := extractSuggestion(aiResult)

				// 5. 写入ES
				if config.IsTrue(cfg.ESEnable) {
					start = time.Now()
					esEvent := esclient.LogEvent{
						EventID:       enhancedEvent.EventID,
						Timestamp:     func() time.Time { t, _ := time.Parse(time.RFC3339, enhancedEvent.Timestamp); return t }(),
						Host:          enhancedEvent.Host,
						Tags:          enhancedEvent.Tags,
						Content:       enhancedEvent.RawText,
						RawLines:      enhancedEvent.RawLines,
						SeverityScore: enhancedEvent.SeverityScore,
						AiResult:      aiResult,
						FilePath:      enhancedEvent.FilePath,
						LineNumber:    enhancedEvent.LineNumber,
						ContextLines:  enhancedEvent.ContextLines,
						IsCellTrace:   enhancedEvent.IsCellTrace,
						ErrorType:     errorType,
						Component:     component,
						StackTrace:    stackTrace,
						Suggestion:    suggestion,
						RelatedEvents: relatedEventIDs,
						ProcessedAt:   time.Now(),
					}

					if err := esClient.IndexLog(esEvent, false); err != nil {
						log.Printf("ES写入失败 [EventID: %s]: %v", enhancedEvent.EventID, err)
						metrics.ESWriteErrorCount.Inc()
						continue
					}
					metrics.ESWriteDuration.Observe(time.Since(start).Seconds())
				}

				// 6. 告警合并策略
				if config.IsTrue(cfg.AlertEnable) {
					send, merged := alertCache.AddOrUpdate(*enhancedEvent, aiResult)
					if send {
						if err := alert.SendWeChat(cfg.WeChatWebhook, merged.Content, merged.AiResult); err != nil {
							log.Printf("告警发送失败 [EventID: %s]: %v", enhancedEvent.EventID, err)
							metrics.AlertSendErrorCount.Inc()
						} else {
							metrics.AlertSentCount.Inc()
						}
					}
				}
			}
		}()
	}
	// 启动 Prometheus 指标服务
	port := cfg.METRICS_PORT
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":"+port, nil)
		if err != nil {
			log.Printf("Failed to start metrics server: %v", err)
		}
	}()

	log.Println("✅ 日志分析服务已启动...")
	log.Printf("✅ Prometheus 指标服务已启动, 端口: %s", port)

	// 主循环
	for {
		select {
		case <-ctx.Done():
			log.Println("正在等待所有任务完成...")
			close(eventChan)
			return
		default:
			// 采集新的日志事件
			events, err := collector.ReadNewLogEvents(cfg.LogFiles)
			if err != nil {
				log.Printf("日志采集失败: %v", err)
				metrics.LogCollectErrorCount.Inc()
				time.Sleep(time.Second * 5)
				continue
			}

			if len(events) > 0 {
				log.Printf("采集到 %d 条新日志事件", len(events))
				metrics.LogEventsCollectedCount.Add(float64(len(events)))

				// 发送事件到处理通道
				for _, event := range events {
					select {
					case eventChan <- &event:
						log.Printf("事件 [%s] 已发送到处理通道", event.EventID)
					case <-ctx.Done():
						return
					}
				}
			}

			// 清理过期的合并告警记录和智能分析器缓存
			if alertCache != nil {
				alertCache.Cleanup()
			}

			smartAnalyzer.CleanupExpiredEvents()

			// 每分钟输出一次统计信息
			if time.Now().Minute()%1 == 0 {
				stats := smartAnalyzer.GetEventStatistics()
				log.Printf("智能分析器统计: %+v", stats)
			}

			// 增加休眠时间，降低CPU使用率
			time.Sleep(time.Second * 5)
		}
	}
}

// 智能字段提取函数

// extractErrorType 根据日志内容和标签智能识别错误类型
func extractErrorType(content string, tags []string) string {
	content = strings.ToUpper(content)

	// 优先级顺序的错误类型映射
	errorTypes := map[string][]string{
		"MEMORY_ERROR":   {"OOM", "OUTOFMEMORY", "MEMORY", "HEAP"},
		"NETWORK_ERROR":  {"CONNECTION REFUSED", "TIMEOUT", "NETWORK", "SOCKET"},
		"DATABASE_ERROR": {"SQL", "DATABASE", "CONNECTION POOL", "DEADLOCK"},
		"AUTHENTICATION": {"AUTH", "LOGIN", "PERMISSION", "UNAUTHORIZED"},
		"CELL_TRACE":     {"CELL TRACE", "TRACE ID"},
		"RUNTIME_ERROR":  {"RUNTIME", "EXCEPTION", "PANIC", "FATAL"},
		"IO_ERROR":       {"FILE", "DISK", "READ", "WRITE"},
		"CONFIGURATION":  {"CONFIG", "PROPERTY", "SETTING"},
		"APPLICATION":    {"ERROR", "FAILED", "EXCEPTION"},
	}

	for errorType, keywords := range errorTypes {
		for _, keyword := range keywords {
			if strings.Contains(content, keyword) {
				return errorType
			}
		}
	}

	// 检查标签
	for _, tag := range tags {
		tag = strings.ToUpper(tag)
		for errorType, keywords := range errorTypes {
			for _, keyword := range keywords {
				if strings.Contains(tag, keyword) {
					return errorType
				}
			}
		}
	}

	return "UNKNOWN"
}

// extractComponent 从文件路径和内容中提取组件名称
func extractComponent(filePath, content string) string {
	if filePath != "" {
		// 从文件路径提取组件名
		dir := filepath.Dir(filePath)
		base := filepath.Base(dir)
		if base != "." && base != "/" {
			return base
		}

		// 从文件名提取
		filename := filepath.Base(filePath)
		if ext := filepath.Ext(filename); ext != "" {
			return strings.TrimSuffix(filename, ext)
		}
		return filename
	}

	// 从内容中提取组件信息
	componentPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)\[(\w+)\]`),          // [ComponentName]
		regexp.MustCompile(`(?i)(\w+)\s*:\s*error`),  // ComponentName: error
		regexp.MustCompile(`(?i)class\s+(\w+\.\w+)`), // class com.example.Service
		regexp.MustCompile(`(?i)service\s+(\w+)`),    // service ServiceName
		regexp.MustCompile(`(?i)module\s+(\w+)`),     // module ModuleName
	}

	for _, pattern := range componentPatterns {
		if matches := pattern.FindStringSubmatch(content); len(matches) > 1 {
			return matches[1]
		}
	}

	return "SYSTEM"
}

// extractStackTrace 从原始日志行中提取堆栈跟踪
func extractStackTrace(rawLines []string) []string {
	var stackTrace []string
	stackKeywords := []string{"at ", "Caused by", "Exception in thread", "Traceback", "File \"", "  File"}

	for _, line := range rawLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 检查是否为堆栈跟踪行
		for _, keyword := range stackKeywords {
			if strings.Contains(line, keyword) {
				stackTrace = append(stackTrace, line)
				break
			}
		}

		// 检查Java堆栈格式
		if strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "    ") {
			if strings.Contains(line, ".") && (strings.Contains(line, "(") || strings.Contains(line, ":")) {
				stackTrace = append(stackTrace, line)
			}
		}
	}

	return stackTrace
}

// extractSuggestion 从AI分析结果中提取建议
func extractSuggestion(aiResult string) string {
	if aiResult == "" {
		return ""
	}

	// 查找建议相关的关键词
	suggestionKeywords := []string{"建议", "解决", "修复", "处理", "suggestion", "recommend", "fix", "solve"}
	lines := strings.Split(aiResult, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		for _, keyword := range suggestionKeywords {
			if strings.Contains(strings.ToLower(line), strings.ToLower(keyword)) {
				return line
			}
		}
	}

	// 如果没有找到特定建议，返回AI结果的前100个字符作为摘要
	if len(aiResult) > 100 {
		return aiResult[:100] + "..."
	}
	return aiResult
}

func getAppName() string {
	return filepath.Base(os.Args[0])
}

func printHelp(appName string) {
	fmt.Printf(`
[%s 使用说明]

配置文件路径：/etc/%s/.env

请按照以下步骤操作：

1. 复制配置模板：
   cp /etc/%s/env.example /etc/%s/.env

2. 编辑 .env 文件以完成配置。

3. 重启服务：
   systemctl restart %s.service
`, appName, appName, appName, appName, appName)
}
