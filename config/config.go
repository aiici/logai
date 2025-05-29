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
	LogFiles      []string
	AIAPIURL      string
	AIAPIKey      string
	AIModel       string
	AIEnable      string
	AlertEnable   string // 是否启用告警
	ESEnable      string // 是否启用ES
	WeChatWebhook string
	ESNodes       []string
	ESIndex       string
	MaxWorkers    int           // 工作池大小
	AlertTTL      time.Duration // 告警缓存TTL
	METRICS_PORT  string
}

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
	METRICS_PORT := os.Getenv("METRICS_PORT")
	if METRICS_PORT == "" {
		METRICS_PORT = "2112"
	}
	ES_ENABLE := os.Getenv("ES_ENABLE")
	if ES_ENABLE == "" {
		ES_ENABLE = "false"
	}
	ALERT_ENABLE := os.Getenv("ALERT_ENABLE")
	if ALERT_ENABLE == "" {
		ALERT_ENABLE = "false"
	}
	AI_ENABLE := os.Getenv("AI_ENABLE")
	if AI_ENABLE == "" {
		AI_ENABLE = "false"
	}

	cfg := &Config{
		LogFiles:      strings.Split(logFilesEnv, ","),
		AIAPIURL:      os.Getenv("AI_API_URL"),
		AIAPIKey:      os.Getenv("AI_API_KEY"),
		AIModel:       os.Getenv("AI_MODEL_NAME"),
		AIEnable:      AI_ENABLE,
		AlertEnable:   ALERT_ENABLE,
		ESEnable:      ES_ENABLE,
		WeChatWebhook: os.Getenv("AI_WECHAT_WEBHOOK"),
		ESNodes:       esNodes,
		ESIndex:       esIndex,
		METRICS_PORT:  METRICS_PORT,
	}

	// 加载可选配置
	if maxWorkersStr := os.Getenv("MAX_WORKERS"); maxWorkersStr != "" {
		maxWorkers, err := strconv.Atoi(maxWorkersStr)
		if err == nil && maxWorkers > 0 {
			cfg.MaxWorkers = maxWorkers
		}
	}

	// 设置告警缓存TTL，默认5分钟
	cfg.AlertTTL = 5 * time.Minute
	if ttlStr := os.Getenv("ALERT_TTL"); ttlStr != "" {
		if ttl, err := time.ParseDuration(ttlStr); err == nil {
			cfg.AlertTTL = ttl
		}
	}

	return cfg, nil
}
func IsTrue(s string) bool {
	return strings.ToLower(strings.TrimSpace(s)) == "true"
}
