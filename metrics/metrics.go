package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// 日志采集相关指标
	LogEventsCollectedCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "log_events_collected_total",
		Help: "采集的日志事件总数",
	})

	LogCollectErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "log_collect_errors_total",
		Help: "日志采集错误次数",
	})

	// AI分析相关指标
	AIAnalysisErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ai_analysis_errors_total",
		Help: "AI分析错误次数",
	})

	AIAnalysisDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ai_analysis_duration_seconds",
		Help:    "AI分析耗时分布",
		Buckets: prometheus.DefBuckets,
	})

	// ES写入相关指标
	ESWriteErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "es_write_errors_total",
		Help: "ES写入错误次数",
	})

	ESWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "es_write_duration_seconds",
		Help:    "ES写入耗时分布",
		Buckets: prometheus.DefBuckets,
	})

	// 告警相关指标
	AlertSentCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerts_sent_total",
		Help: "发送的告警总数",
	})

	AlertSendErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alert_send_errors_total",
		Help: "告警发送错误次数",
	})

	AlertMergedCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerts_merged_total",
		Help: "合并的告警总数",
	})

	// Cell Trace相关指标
	CellTraceErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cell_trace_errors_total",
		Help: "Cell Trace异常总数",
	})

	CellTraceErrorSeverity = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cell_trace_error_severity",
		Help:    "Cell Trace异常严重性分布",
		Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
)
