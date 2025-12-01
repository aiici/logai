package collector

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

// 带上下文的文件读取函数
func readFromFileWithContext(ctx context.Context, filePath string, config CollectorConfig) ([]LogEvent, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	lastOffset := loadOffset(filePath)
	_, err = file.Seek(lastOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	var events []LogEvent
	var allLines []string
	var lineNumbers []int
	lineNum := 0

	// 首先读取所有行，用于上下文提取
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return events, ctx.Err()
		default:
		}
		lineNum++
		allLines = append(allLines, scanner.Text())
		lineNumbers = append(lineNumbers, lineNum)
	}

	// 查找匹配的行
	var buffer []string
	var bufferLineNums []int
	var matched bool
	var matchStartLine int

	for i, line := range allLines {
		isMatch, isCellTrace := isLineMatch(line)
		if isMatch {
			if len(buffer) > 0 && matched {
				// 处理前一个事件
				event := toLogEventWithContext(buffer, bufferLineNums, filePath, matchStartLine, allLines, config.ContextLines)
				event.IsCellTrace = isCellTrace
				events = append(events, event)
			}
			buffer = []string{line}
			bufferLineNums = []int{lineNumbers[i]}
			matchStartLine = lineNumbers[i]
			matched = true
		} else if matched {
			// 继续收集相关行，特别是Cell Trace的完整堆栈
			if shouldIncludeLine(line, buffer) {
				buffer = append(buffer, line)
				bufferLineNums = append(bufferLineNums, lineNumbers[i])
			}
		}
	}

	// 处理最后一个事件
	if matched && len(buffer) > 0 {
		event := toLogEventWithContext(buffer, bufferLineNums, filePath, matchStartLine, allLines, config.ContextLines)
		_, event.IsCellTrace = isLineMatch(buffer[0])
		events = append(events, event)
	}

	offset, _ := file.Seek(0, io.SeekCurrent)
	saveOffset(filePath, offset)
	return events, nil
}

// 兼容性函数
func readFromFile(filePath string) ([]LogEvent, error) {
	ctx := context.Background()
	return readFromFileWithContext(ctx, filePath, DefaultConfig)
}

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

// 兼容Windows路径
func sanitizeFileName(path string) string {
	s := filepath.ToSlash(path)          // 统一分隔符
	s = strings.ReplaceAll(s, "/", "_")  // 转换路径为文件名
	s = strings.ReplaceAll(s, ":", "_")  // 替换 Windows 的冒号
	s = strings.ReplaceAll(s, "\\", "_") // 额外处理反斜杠
	return s
}

// 增强的行匹配函数，支持Cell Trace特殊检测
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

// 判断是否应该包含某行（用于收集完整的错误堆栈）
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

// 带上下文的日志事件创建函数
func toLogEventWithContext(lines []string, lineNumbers []int, filePath string, startLine int, allLines []string, contextLines int) LogEvent {
	host, _ := os.Hostname()
	text := strings.Join(lines, "\n")
	tags := extractTags(lines)
	score := calculateSeverityScore(lines, tags)
	eventID := ExtractEventID(lines)

	// 提取上下文行
	contextBefore, contextAfter := extractContext(allLines, startLine-1, contextLines)
	contextLinesResult := append(contextBefore, contextAfter...)

	// 添加调试日志
	// fmt.Printf("生成事件: EventID=%s, 内容前20字符=%s\n", eventID, getFirstNChars(text, 20))

	return LogEvent{
		RawLines:      lines,
		RawText:       text,
		Timestamp:     time.Now().Format(time.RFC3339),
		Host:          host,
		Tags:          tags,
		SeverityScore: score,
		EventID:       eventID,
		FilePath:      filePath,
		LineNumber:    startLine,
		ContextLines:  contextLinesResult,
		IsCellTrace:   false, // 将在调用处设置
	}
}

// 兼容性函数
func toLogEvent(lines []string) LogEvent {
	return toLogEventWithContext(lines, nil, "", 0, nil, 0)
}

// 增强的严重性评分计算
func calculateSeverityScore(lines []string, tags []string) int {
	score := 0
	maxScore := 0

	// 基于标签计算分数
	for _, tag := range tags {
		upperTag := strings.ToUpper(tag)
		if s, ok := severityMap[upperTag]; ok {
			score += s
			if s > maxScore {
				maxScore = s
			}
		}
	}

	// 检查特殊模式加分
	text := strings.Join(lines, " ")
	upperText := strings.ToUpper(text)

	// Cell Trace异常额外加分
	for _, pattern := range cellTracePatterns {
		if pattern.MatchString(text) {
			score += 3
			break
		}
	}

	// 包含堆栈跟踪加分
	if strings.Contains(upperText, "STACK") || strings.Contains(upperText, "TRACEBACK") {
		score += 2
	}

	// 内核Call Trace异常额外加分
	if strings.Contains(text, "Call Trace:") || strings.Contains(upperText, "CALL TRACE") {
		score += 4
	}

	// 包含<TASK>和</TASK>标记加分
	if strings.Contains(text, "<TASK>") && strings.Contains(text, "</TASK>") {
		score += 3
	}

	// 包含多个错误关键词加分
	errorCount := 0
	for _, kw := range []string{"ERROR", "EXCEPTION", "FAILED", "FATAL", "SEGMENTATION FAULT", "CORE DUMPED", "CALL TRACE", "BLOCKED FOR MORE THAN"} {
		if strings.Contains(upperText, kw) {
			errorCount++
		}
	}
	if errorCount > 1 {
		score += errorCount
	}

	// 检查是否包含JSON格式的错误信息
	if strings.Contains(text, "\"level\":\"error\"") || strings.Contains(text, "\"error\":") {
		score += 3
	}

	// 检查是否为内核hung task问题
	if strings.Contains(text, "blocked for more than") && strings.Contains(text, "hung_task_timeout_secs") {
		score += 5
	}

	// 确保最小分数为最高单项分数
	if score < maxScore {
		score = maxScore
	}

	// 限制最高分数
	if score > 10 {
		score = 10
	}

	return score
}

// 提取上下文行
func extractContext(allLines []string, centerIndex, contextLines int) ([]string, []string) {
	if len(allLines) == 0 || contextLines <= 0 {
		return nil, nil
	}

	var before, after []string

	// 提取前面的行
	start := centerIndex - contextLines
	if start < 0 {
		start = 0
	}
	for i := start; i < centerIndex && i < len(allLines); i++ {
		before = append(before, allLines[i])
	}

	// 提取后面的行
	end := centerIndex + contextLines + 1
	if end > len(allLines) {
		end = len(allLines)
	}
	for i := centerIndex + 1; i < end && i < len(allLines); i++ {
		after = append(after, allLines[i])
	}

	return before, after
}

func extractTags(lines []string) []string {
	tags := []string{}
	for _, line := range lines {
		for _, kw := range keywords {
			if strings.Contains(strings.ToUpper(line), strings.ToUpper(kw)) {
				tags = append(tags, kw)
			}
		}
	}
	return tags
}

// 提取TraceID或者RequestID
func ExtractEventID(lines []string) string {
	for _, line := range lines {
		if strings.Contains(line, "TraceID:") {
			parts := strings.Split(line, "TraceID:")
			if len(parts) > 1 {
				id := strings.Fields(parts[1])[0]
				return id
			}
		}
		if strings.Contains(line, "RequestID:") {
			parts := strings.Split(line, "RequestID:")
			if len(parts) > 1 {
				id := strings.Fields(parts[1])[0]
				return id
			}
		}
	}

	// 如果没有找到明确的ID，基于日志内容生成一个稳定的哈希ID
	if len(lines) > 0 {
		content := strings.Join(lines, "\n")
		// 移除时间戳等变化的部分，保留稳定的特征
		content = removeTimestamps(content)
		// 使用简单的哈希算法生成ID
		return generateStableID(content)
	}
	return ""
}

// removeTimestamps 移除日志中的时间戳，使内容更稳定
func removeTimestamps(content string) string {
	// 移除常见的时间戳格式
	re := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.\d]*[+-]\d{2}:\d{2}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[,.]\d{3}|\w{3} \d{1,2} \d{2}:\d{2}:\d{2}`)
	content = re.ReplaceAllString(content, "")
	return content
}

// generateStableID 基于内容生成稳定的ID
func generateStableID(content string) string {
	// 使用MD5哈希生成稳定的ID
	// 首先标准化内容，移除变化的部分
	normalized := normalizeContent(content)

	// 计算MD5哈希
	hash := md5.Sum([]byte(normalized))
	return hex.EncodeToString(hash[:])
}

// normalizeContent 标准化内容，移除变化的部分
func normalizeContent(content string) string {
	// 移除时间戳
	content = removeTimestamps(content)

	// 移除可能变化的数字（如PID、端口号等）
	re := regexp.MustCompile(`\b\d+\b`)
	content = re.ReplaceAllString(content, "NUMBER")

	// 转换为小写以提高一致性
	content = strings.ToLower(content)

	// 移除多余的空白字符
	content = regexp.MustCompile(`\s+`).ReplaceAllString(content, " ")

	return strings.TrimSpace(content)
}

// getFirstNChars 获取字符串的前n个字符
func getFirstNChars(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
