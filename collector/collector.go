package collector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 注意：文件读取和处理相关的函数已移到 processor.go 文件中
// 包括：readFromFileWithContext, readFromFile, toLogEventWithContext, toLogEvent 等函数

// 扩展关键词匹配，支持正则表达式
var keywords = []string{"ERROR", "FAILED", "Failed", "Error", "fail", "error", "oom", "killed", "OOM", "KILLED", "Cell Trace", "Runtime Error", "Exception", "Panic", "Fatal", "Critical", "Timeout", "Connection refused", "OutOfMemory", "Segmentation fault", "core dumped", "Blocked for more than", "hung_task_timeout_secs"}

// 严重性评分系统
var severityMap = map[string]int{
	"FATAL":                  10,
	"CRITICAL":               9,
	"ERROR":                  8,
	"EXCEPTION":              7,
	"PANIC":                  7,
	"OOM":                    8,
	"OUTOFMEMORY":            8,
	"KILLED":                 6,
	"FAILED":                 5,
	"FAIL":                   4,
	"CELL TRACE":             6,
	"RUNTIME ERROR":          5,
	"TIMEOUT":                4,
	"CONNECTION REFUSED":     3,
	"HUNG_TASK_TIMEOUT_SECS": 5,
	"SEGMENTATION FAULT":     9,
	"CORE DUMPED":            8,
	"BLOCKED FOR MORE THAN":  8,
}

// Cell Trace 异常的特殊模式
var cellTracePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)cell\s+trace.*error`),
	regexp.MustCompile(`(?i)cell\s+trace.*exception`),
	regexp.MustCompile(`(?i)cell\s+trace.*failed`),
	regexp.MustCompile(`(?i)trace\s+id:\s*[a-zA-Z0-9-]+.*error`),
}

const (
	offsetFilePrefix = ".last_offset_"
	offsetDirPath    = "./offsets" // 存储偏移量文件的目录
)

// 增强的日志事件结构
type LogEvent struct {
	RawLines      []string
	RawText       string
	Timestamp     string
	Host          string
	Tags          []string
	SeverityScore int
	EventID       string
	FilePath      string   // 添加文件路径
	LineNumber    int      // 添加行号
	ContextLines  []string // 添加上下文行
	IsCellTrace   bool     // 标识是否为Cell Trace异常
}

// 并行采集配置
type CollectorConfig struct {
	MaxWorkers   int           // 最大并发数
	ContextLines int           // 上下文行数
	BufferSize   int           // 缓冲区大小
	Timeout      time.Duration // 超时时间
}

// 默认配置
var DefaultConfig = CollectorConfig{
	MaxWorkers:   10,
	ContextLines: 5,
	BufferSize:   1000,
	Timeout:      30 * time.Second,
}

// 并行多日志文件采集，支持超时和错误处理
func ReadNewLogEvents(filePaths []string) ([]LogEvent, error) {
	return ReadNewLogEventsWithConfig(filePaths, DefaultConfig)
}

// 使用自定义配置的并行日志采集
func ReadNewLogEventsWithConfig(filePaths []string, config CollectorConfig) ([]LogEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	// 创建工作池
	workerCount := config.MaxWorkers
	if len(filePaths) < workerCount {
		workerCount = len(filePaths)
	}

	fileChan := make(chan string, len(filePaths))
	resultChan := make(chan []LogEvent, len(filePaths))
	errorChan := make(chan error, len(filePaths))

	// 启动工作协程
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case filePath, ok := <-fileChan:
					if !ok {
						return
					}
					events, err := readFromFileWithContext(ctx, filePath, config)
					if err != nil {
						errorChan <- fmt.Errorf("读取文件 %s 失败: %v", filePath, err)
						continue
					}
					resultChan <- events
				case <-ctx.Done():
					errorChan <- fmt.Errorf("采集超时")
					return
				}
			}
		}()
	}

	// 发送文件路径到工作队列
	go func() {
		for _, path := range filePaths {
			fileChan <- path
		}
		close(fileChan)
	}()

	// 等待所有工作完成
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// 收集结果
	var allEvents []LogEvent
	var errors []error

	for i := 0; i < len(filePaths); i++ {
		select {
		case events := <-resultChan:
			allEvents = append(allEvents, events...)
		case err := <-errorChan:
			errors = append(errors, err)
		case <-ctx.Done():
			return allEvents, fmt.Errorf("采集超时")
		}
	}

	// 如果有错误但也有成功的结果，记录错误但返回成功的部分
	if len(errors) > 0 {
		fmt.Printf("部分文件读取失败: %v\n", errors)
	}

	return allEvents, nil
}

// loadOffset loads the last read offset for a file
func loadOffset(filePath string) int64 {
	// 确保offset目录存在
	if err := os.MkdirAll(offsetDirPath, 0755); err != nil {
		fmt.Printf("创建offset目录失败: %v\n", err)
		return 0
	}

	offsetFile := filepath.Join(offsetDirPath, offsetFilePrefix+sanitizeFileName(filePath))
	data, err := os.ReadFile(offsetFile)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Printf("读取offset文件失败: %v\n", err)
		}
		return 0
	}

	offset, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		fmt.Printf("解析offset值失败: %v\n", err)
		return 0
	}

	return offset
}

// saveOffset saves the current read offset for a file
func saveOffset(filePath string, offset int64) {
	// 确保offset目录存在
	if err := os.MkdirAll(offsetDirPath, 0755); err != nil {
		fmt.Printf("创建offset目录失败: %v\n", err)
		return
	}

	offsetFile := filepath.Join(offsetDirPath, offsetFilePrefix+sanitizeFileName(filePath))
	if err := os.WriteFile(offsetFile, []byte(fmt.Sprintf("%d", offset)), 0644); err != nil {
		fmt.Printf("保存offset失败: %v\n", err)
	}
}

// sanitizeFileName sanitizes a file path for use as a filename
func sanitizeFileName(path string) string {
	s := filepath.ToSlash(path)          // 统一分隔符
	s = strings.ReplaceAll(s, "/", "_")  // 转换路径为文件名
	s = strings.ReplaceAll(s, ":", "_")  // 替换 Windows 的冒号
	s = strings.ReplaceAll(s, "\\", "_") // 额外处理反斜杠
	return s
}

// isLineMatch checks if a line matches any of the defined patterns
func isLineMatch(line string) (bool, bool) {
	// 检查是否为Cell Trace异常
	for _, pattern := range cellTracePatterns {
		if pattern.MatchString(line) {
			return true, true
		}
	}

	// 检查普通关键词
	for _, kw := range keywords {
		if strings.Contains(strings.ToUpper(line), strings.ToUpper(kw)) {
			return true, false
		}
	}

	// 检查是否为JSON格式的日志且包含错误信息
	if strings.HasPrefix(strings.TrimSpace(line), "{") && strings.Contains(line, "error") {
		return true, false
	}

	return false, false
}

// shouldIncludeLine determines if a line should be included in the event context
func shouldIncludeLine(line string, buffer []string) bool {
	// 如果是空行，跳过
	if strings.TrimSpace(line) == "" {
		return false
	}

	// 如果包含堆栈跟踪相关的关键词
	stackTraceKeywords := []string{"at ", "Caused by", "\t", "    ", "Exception in thread", "java.", "org.", "com.", "Traceback", "File \"", "line "}
	for _, kw := range stackTraceKeywords {
		if strings.Contains(line, kw) {
			return true
		}
	}

	// 特殊处理内核日志的Call Trace
	// 检查是否为内核Call Trace的相关行
	if len(buffer) > 0 {
		firstLine := buffer[0]
		// 如果第一个匹配行包含Call Trace关键词
		if strings.Contains(firstLine, "Call Trace:") || strings.Contains(firstLine, "call trace") {
			// 包含以下特征的行都应该收集
			kernelTraceIndicators := []string{"<TASK>", "</TASK>", "+0x", "/", "RIP:", "RSP:", "RAX:", "RBX:", "RCX:", "RDX:", "RSI:", "RDI:", "RBP:", "R8:", "R9:", "R10:", "R11:", "R12:", "R13:", "R14:", "R15:"}
			for _, indicator := range kernelTraceIndicators {
				if strings.Contains(line, indicator) {
					return true
				}
			}

			// 如果行以函数名+偏移量格式开头，也应该收集
			// 例如: __schedule+0x2a9/0x8b0
			if strings.Contains(line, "+0x") && strings.Contains(line, "/") {
				return true
			}

			// 如果行包含寄存器信息，也应该收集
			if strings.Contains(line, ":") && (strings.Contains(line, "0x") || strings.Contains(line, "ffff")) {
				return true
			}
		}
	}

	// 如果是Cell Trace相关的后续行
	if len(buffer) > 0 {
		firstLine := buffer[0]
		for _, pattern := range cellTracePatterns {
			if pattern.MatchString(firstLine) {
				// Cell Trace异常，收集更多上下文
				if strings.Contains(strings.ToLower(line), "trace") ||
					strings.Contains(strings.ToLower(line), "cell") ||
					strings.Contains(line, ":") {
					return true
				}
			}
		}
	}

	return false
}

// 注意：以下函数已移到 processor.go 文件中
// - calculateSeverityScore
// - extractContext
// - extractTags
// - ExtractEventID
// - removeTimestamps
// - generateStableID
// - normalizeContent
// - getFirstNChars
