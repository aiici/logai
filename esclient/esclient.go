package esclient

import (
	"context"
	"fmt"
	"time"

	"github.com/olivere/elastic/v7"
)

// ESClient 封装了 elastic.Client 和索引前缀
type ESClient struct {
	client *elastic.Client
	index  string
}

// NewESClient 支持多个节点初始化
func NewESClient(nodes []string, indexPrefix string) (*ESClient, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(nodes...),
		elastic.SetSniff(false), // 关闭 sniff，适配容器或多节点
	)
	if err != nil {
		return nil, fmt.Errorf("创建ES客户端失败: %w", err)
	}

	return &ESClient{
		client: client,
		index:  indexPrefix,
	}, nil
}

// LogEvent 为结构化日志模型，支持 AI 分析与告警分数
type LogEvent struct {
	EventID       string    `json:"event_id"`   // 可用于日志聚合或唯一识别
	Timestamp     time.Time `json:"@timestamp"` // 兼容 Kibana 时间字段
	Host          string    `json:"host"`
	Tags          []string  `json:"tags,omitempty"`
	Content       string    `json:"content"`
	SeverityScore int       `json:"severity_score"` // 日志异常等级打分
	AiResult      string    `json:"ai_result"`      // AI 分析内容摘要
}

// IndexLog 将日志事件写入 ES（每日索引）
func (e *ESClient) IndexLog(event LogEvent) error {
	indexName := fmt.Sprintf("%s-%s", e.index, time.Now().Format("2006.01.02"))
	_, err := e.client.Index().
		Index(indexName).
		BodyJson(event).
		Do(context.Background())
	if err != nil {
		return fmt.Errorf("写入ES失败: %w", err)
	}
	return nil
}
