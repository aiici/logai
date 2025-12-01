package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	LogFiles       []string
	AIAPIURL       string
	AIAPIKey       string
	AIModel        string
	AIEnable       string
	WeChatWebhook  string
	ESNodes        []string
	ESIndex        string
	MaxWorkers     int           // 工作池大小
	AlertTTL       time.Duration // 告警缓存TTL
	METRICS_PORT   string
	LogLevel       string        // 日志级别
	EnableCellTrace bool         // 是否启用Cell Trace检测
	EnableAlert    bool          // 是否启用告警功能
	EnableES       bool          // 是否启用ES存储功能
}

// Load 加载配置
func Load() (*Config, error) {
	_ = godotenv.Load()

	logFilesEnv := os.Getenv("LOG_FILE_PATHS")
	if logFilesEnv == "" {
		return nil, fmt.Errorf("❌ 缺少必要环境变量 LOG_FILE_PATHS")
	}

	esNodes := strings.Split(os.Getenv("ES_NODES"), ",")
	esIndex := os.Getenv("ES_INDEX")
	if len(esNodes) == 0 || esIndex == "" {
		return nil, fmt.Errorf("❌ 缺少 Elasticsearch 配置: ES_NODES 或 ES_INDEX")
	}
	
	// 验证ES节点URL格式
	for i, node := range esNodes {
		if node == "" {
			continue
		}
		if !strings.HasPrefix(node, "http://") && !strings.HasPrefix(node, "https://") {
			esNodes[i] = "http://" + node
		}
	}

	METRICS_PORT := os.Getenv("METRICS_PORT")
	if METRICS_PORT == "" {
		METRICS_PORT = "2112"
	}

	cfg := &Config{
		LogFiles:       strings.Split(logFilesEnv, ","),
		AIAPIURL:       os.Getenv("AI_API_URL"),
		AIAPIKey:       os.Getenv("AI_API_KEY"),
		AIModel:        os.Getenv("AI_MODEL_NAME"),
		AIEnable:       os.Getenv("AI_ENABLE"),
		WeChatWebhook:  os.Getenv("AI_WECHAT_WEBHOOK"),
		ESNodes:        esNodes,
		ESIndex:        esIndex,
		METRICS_PORT:   METRICS_PORT,
		LogLevel:       "info", // 默认日志级别
		EnableCellTrace: true,  // 默认启用Cell Trace检测
	}

	// 加载可选配置
	if maxWorkersStr := os.Getenv("MAX_WORKERS"); maxWorkersStr != "" {
		if maxWorkers, err := strconv.Atoi(maxWorkersStr); err == nil && maxWorkers > 0 {
			cfg.MaxWorkers = maxWorkers
		}
	}

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		cfg.LogLevel = logLevel
	}

	if enableCellTrace := os.Getenv("ENABLE_CELL_TRACE"); enableCellTrace != "" {
		cfg.EnableCellTrace = strings.ToLower(enableCellTrace) == "true"
	}

	// 设置是否启用告警功能，默认启用
	cfg.EnableAlert = true
	if enableAlert := os.Getenv("ENABLE_ALERT"); enableAlert != "" {
		cfg.EnableAlert = strings.ToLower(enableAlert) == "true"
	}

	// 设置是否启用ES存储功能，默认启用
	cfg.EnableES = true
	if enableES := os.Getenv("ENABLE_ES"); enableES != "" {
		cfg.EnableES = strings.ToLower(enableES) == "true"
	}

	// 设置告警缓存TTL，默认5分钟
	cfg.AlertTTL = 5 * time.Minute
	if ttlStr := os.Getenv("ALERT_TTL"); ttlStr != "" {
		if ttl, err := time.ParseDuration(ttlStr); err == nil {
			cfg.AlertTTL = ttl
		}
	}

	// 验证必要配置
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validate 验证配置的有效性
func (c *Config) validate() error {
	if len(c.LogFiles) == 0 {
		return fmt.Errorf("日志文件路径不能为空")
	}

	// 验证日志文件路径是否存在
	for _, path := range c.LogFiles {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			// 不强制要求文件必须存在，因为可能是动态创建的
			fmt.Printf("警告: 日志文件不存在: %s\n", path)
		}
	}

	// 如果启用了AI分析，验证必要配置
	if strings.ToLower(c.AIEnable) == "true" {
		if c.AIAPIURL == "" {
			return fmt.Errorf("启用AI分析时必须配置 AI_API_URL")
		}
		if c.AIAPIKey == "" {
			return fmt.Errorf("启用AI分析时必须配置 AI_API_KEY")
		}
		if c.AIModel == "" {
			return fmt.Errorf("启用AI分析时必须配置 AI_MODEL_NAME")
		}
	}

	// 验证企业微信webhook
	if c.WeChatWebhook != "" && !strings.HasPrefix(c.WeChatWebhook, "http") {
		return fmt.Errorf("企业微信webhook地址必须是有效的URL")
	}

	return nil
}
