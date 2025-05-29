package esclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
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
	EventID       string    `json:"event_id"`
	Timestamp     time.Time `json:"@timestamp"`
	Host          string    `json:"host"`
	Tags          []string  `json:"tags,omitempty"`
	Content       string    `json:"content"`
	RawLines      []string  `json:"raw_lines,omitempty"`
	SeverityScore int       `json:"severity_score"`
	AiResult      string    `json:"ai_result"`

	FilePath     string   `json:"file_path,omitempty"`
	LineNumber   int      `json:"line_number,omitempty"`
	ContextLines []string `json:"context_lines,omitempty"`
	IsCellTrace  bool     `json:"is_cell_trace"`

	ErrorType     string    `json:"error_type,omitempty"`
	Component     string    `json:"component,omitempty"`
	StackTrace    []string  `json:"stack_trace,omitempty"`
	Suggestion    string    `json:"suggestion,omitempty"`
	RelatedEvents []string  `json:"related_events,omitempty"`
	ProcessedAt   time.Time `json:"processed_at"`
}

// getIndexName 返回每日索引名或 alias
func (e *ESClient) getIndexName(useAlias bool) string {
	if useAlias {
		return e.index
	}
	return fmt.Sprintf("%s-%s", e.index, time.Now().Format("2006.01.02"))
}

// IndexLog 将日志事件写入 ES（支持使用 alias）
func (e *ESClient) IndexLog(event LogEvent, useAlias bool) error {
	indexName := e.getIndexName(useAlias)
	_, err := e.client.Index().
		Index(indexName).
		BodyJson(event).
		Do(context.Background())
	if err != nil {
		return fmt.Errorf("写入ES失败: %w", err)
	}
	return nil
}

// IndexLogWithCtx 上下文支持（终止控制）
func (e *ESClient) IndexLogWithCtx(ctx context.Context, event LogEvent, useAlias bool) error {
	indexName := e.getIndexName(useAlias)
	_, err := e.client.Index().
		Index(indexName).
		BodyJson(event).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("写入ES失败: %w", err)
	}
	return nil
}

// IndexLogWithRetry 带重试机制的写入
func (e *ESClient) IndexLogWithRetry(event LogEvent, useAlias bool) error {
	indexName := e.getIndexName(useAlias)
	operation := func() error {
		_, err := e.client.Index().
			Index(indexName).
			BodyJson(event).
			Do(context.Background())
		return err
	}
	err := backoff.Retry(operation, backoff.NewExponentialBackOff())
	if err != nil {
		return fmt.Errorf("写入ES失败（重试后仍失败）: %w", err)
	}
	return nil
}

// BulkIndexLogs 批量写入日志（推荐用于高频场景）
func (e *ESClient) BulkIndexLogs(events []LogEvent, useAlias bool) error {
	indexName := e.getIndexName(useAlias)
	bulk := e.client.Bulk()

	for _, event := range events {
		req := elastic.NewBulkIndexRequest().
			Index(indexName).
			Doc(event)
		bulk = bulk.Add(req)
	}

	resp, err := bulk.Do(context.Background())
	if err != nil {
		return fmt.Errorf("批量写入ES失败: %w", err)
	}
	if resp.Errors {
		return fmt.Errorf("批量写入ES部分失败")
	}

	return nil
}
func IsTrue(s string) bool {
	return strings.ToLower(strings.TrimSpace(s)) == "true"
}
