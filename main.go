package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log-ai-analyzer/ai"
	"log-ai-analyzer/alert"
	"log-ai-analyzer/collector"
	"log-ai-analyzer/config"
	"log-ai-analyzer/esclient"
	"log-ai-analyzer/metrics"
	"log-ai-analyzer/processor"
	"net/http"
)

func main() {
	// 1. 加载配置
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 设置日志前缀
	log.SetPrefix("[LogAI] ")
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 打印系统信息
	log.Printf("系统启动中... Go版本: %s, CPU核心数: %d", runtime.Version(), runtime.NumCPU())

	// 2. 初始化ES客户端
	esClient, err := esclient.NewESClient(cfg.ESNodes, cfg.ESIndex)
	if err != nil {
		log.Fatalf("初始化ES客户端失败: %v", err)
	}
	log.Println("✅ Elasticsearch客户端初始化成功")

	// 3. 初始化告警缓存
	alertCache := alert.NewAlertCache(cfg.AlertTTL)
	log.Println("✅ 告警缓存初始化成功")

	// 4. 设置优雅退出
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

	log.Printf("启动工作池，工作协程数: %d", workerCount)

	// 创建事件处理通道
	eventChan := make(chan *collector.LogEvent, 100)

	// 启动工作池
	for i := 0; i < workerCount; i++ {
		go worker(ctx, cfg, esClient, alertCache, eventChan, i)
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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("正在等待所有任务完成...")
			close(eventChan)
			// 等待一段时间确保所有任务完成
			time.Sleep(2 * time.Second)
			log.Println("服务已优雅退出")
			return
		case <-ticker.C:
			// 采集新的日志事件
			events, err := collector.ReadNewLogEvents(cfg.LogFiles)
			if err != nil {
				log.Printf("日志采集失败: %v", err)
				metrics.LogCollectErrorCount.Inc()
				continue
			}

			if len(events) > 0 {
				metrics.LogEventsCollectedCount.Add(float64(len(events)))
				log.Printf("发现 %d 个新的日志事件", len(events))

				// 发送事件到处理通道
				for _, event := range events {
					select {
					case eventChan <- &event:
						// 事件已发送到处理通道
					case <-ctx.Done():
						return
					}
				}
			}

			// 清理过期的合并告警记录
			alertCache.Cleanup()
		}
	}
}

// worker 工作协程处理日志事件
func worker(ctx context.Context, cfg *config.Config, esClient *esclient.ESClient, alertCache *alert.AlertCache, eventChan <-chan *collector.LogEvent, workerID int) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("工作协程 #%d 正在退出...", workerID)
			return
		case event := <-eventChan:
			if event == nil {
				continue
			}

			log.Printf("工作协程 #%d 开始处理事件 [EventID: %s]", workerID, event.EventID)

			// 1. 数据脱敏
			event.RawText = processor.MaskSensitiveInfo(event.RawText)

			// 2. AI分析
			start := time.Now()
			aiResult, err := ai.Analyze(cfg, event.RawText)
			metrics.AIAnalysisDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				log.Printf("AI分析失败 [EventID: %s]: %v", event.EventID, err)
				metrics.AIAnalysisErrorCount.Inc()
				// 即使AI分析失败，也继续处理其他步骤
				aiResult = fmt.Sprintf("AI分析失败: %v", err)
			}

			// 3. 写入ES
			if cfg.EnableES {
				start = time.Now()
				timestamp, err := time.Parse(time.RFC3339, event.Timestamp)
				if err != nil {
					// 如果RFC3339格式解析失败，尝试其他格式
					timestamp = time.Now()
				}

				if err := esClient.IndexLog(esclient.LogEvent{
					EventID:       event.EventID,
					Timestamp:     timestamp,
					Host:          event.Host,
					Tags:          event.Tags,
					Content:       event.RawText,
					SeverityScore: event.SeverityScore,
					AiResult:      aiResult,
				}); err != nil {
					log.Printf("ES写入失败 [EventID: %s]: %v", event.EventID, err)
					metrics.ESWriteErrorCount.Inc()
					metrics.EventProcessErrorCount.Inc()
					continue
				}
				metrics.ESWriteDuration.Observe(time.Since(start).Seconds())
				metrics.ESWriteSuccessCount.Inc()
			} else {
				log.Printf("ES存储功能已禁用，跳过写入 [EventID: %s]", event.EventID)
				// 即使禁用了ES，也认为事件处理成功
				metrics.EventProcessSuccessCount.Inc()
			}

			// 4. 告警合并策略
			send, merged := alertCache.AddOrUpdate(*event, aiResult)
			if send {
				// 检查是否启用告警功能
				if cfg.EnableAlert {
					if cfg.WeChatWebhook != "" {
						if err := alert.SendWeChat(cfg.WeChatWebhook, merged.Content, merged.AiResult); err != nil {
							log.Printf("告警发送失败 [EventID: %s]: %v", event.EventID, err)
							metrics.AlertSendErrorCount.Inc()
							metrics.EventProcessErrorCount.Inc()
						} else {
							log.Printf("告警发送成功 [EventID: %s]", event.EventID)
							metrics.AlertSentCount.Inc()
							metrics.EventProcessSuccessCount.Inc()
						}
					} else {
						log.Printf("跳过告警发送，未配置Webhook [EventID: %s]", event.EventID)
						metrics.AlertSkipCount.Inc()
						metrics.EventProcessSuccessCount.Inc()
					}
				} else {
					log.Printf("告警功能已禁用，跳过发送 [EventID: %s]", event.EventID)
					metrics.AlertSkipCount.Inc()
					metrics.EventProcessSuccessCount.Inc()
				}
			} else {
				// 事件处理成功但不需要发送告警
				metrics.EventProcessSuccessCount.Inc()
			}

			log.Printf("工作协程 #%d 完成处理事件 [EventID: %s]", workerID, event.EventID)
		}
	}
}
