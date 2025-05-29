package collector

import (
	"bufio"
	"context"
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
var keywords = []string{"ERROR", "FAILED", "Failed", "Error", "fail", "error", "oom", "killed", "OOM", "KILLED", "Cell Trace", "Runtime Error", "Exception", "Panic", "Fatal", "Critical", "Timeout", "Connection refused", "OutOfMemory"}

// 严重性评分系统
var severityMap = map[string]int{
	"FATAL":              10,
	"CRITICAL":           9,
	"ERROR":              8,
	"EXCEPTION":          7,
	"PANIC":              7,
	"OOM":                8,
	"OUTOFMEMORY":        8,
	"KILLED":             6,
	"FAILED":             5,
	"FAIL":               4,
	"CELL TRACE":         6,
	"RUNTIME ERROR":      5,
	"TIMEOUT":            4,
	"CONNECTION REFUSED": 3,
	"WARNING":            2,
	"WARN":               2,
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

	// 获取文件大小
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	lastOffset := loadOffset(filePath)
	// 如果文件大小小于等于上次的偏移量，说明没有新内容
	if fileInfo.Size() <= lastOffset {
		return nil, nil
	}

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
	var lastErrorLine string // 用于检测重复错误

	for i, line := range allLines {
		isMatch, isCellTrace := isLineMatch(line)
		// 检查是否与上一个错误消息相似
		if isMatch && isSimilarError(line, lastErrorLine) {
			continue // 跳过相似的错误消息
		}

		if isMatch {
			// 保存当前错误消息用于后续比较
			lastErrorLine = line

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
			} else {
				// 如果不应该包含该行，说明当前错误消息已经结束
				matched = false
				// 处理当前缓冲区中的事件
				if len(buffer) > 0 {
					event := toLogEventWithContext(buffer, bufferLineNums, filePath, matchStartLine, allLines, config.ContextLines)
					event.IsCellTrace = isCellTrace
					events = append(events, event)
					buffer = nil
					bufferLineNums = nil
				}
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
	return false, false
}

// 判断是否应该包含某行（用于收集完整的错误堆栈）
func shouldIncludeLine(line string, buffer []string) bool {
	// 如果是空行，跳过
	if strings.TrimSpace(line) == "" {
		return false
	}

	// 如果缓冲区为空，不应该包含任何行
	if len(buffer) == 0 {
		return false
	}

	// 获取缓冲区的第一行作为参考
	firstLine := buffer[0]

	// 检查是否为堆栈跟踪
	stackTraceKeywords := []string{"at ", "Caused by", "Exception in thread", "Traceback", "File \""}
	indentationMarkers := []string{"\t", "    "}

	// 检查是否为缩进的堆栈信息
	isIndented := false
	for _, marker := range indentationMarkers {
		if strings.HasPrefix(line, marker) {
			isIndented = true
			break
		}
	}

	// 如果是缩进的行，检查是否包含Java包名或Python文件路径
	if isIndented {
		packageKeywords := []string{"java.", "org.", "com.", "net.", "io.", "/", ".py", ".java"}
		for _, pkg := range packageKeywords {
			if strings.Contains(line, pkg) {
				return true
			}
		}
	}

	// 检查是否包含堆栈跟踪关键词
	for _, kw := range stackTraceKeywords {
		if strings.Contains(line, kw) {
			return true
		}
	}

	// 特殊处理Cell Trace相关的行
	if isCellTraceLine(firstLine) {
		// 只包含与Cell Trace直接相关的信息
		return strings.Contains(strings.ToLower(line), "trace id") ||
			(strings.Contains(line, ":") && strings.Contains(strings.ToLower(line), "error"))
	}

	return false
}

// 判断是否为Cell Trace行
func isCellTraceLine(line string) bool {
	for _, pattern := range cellTracePatterns {
		if pattern.MatchString(line) {
			return true
		}
	}
	return false
}

// 判断两个错误消息是否相似
func isSimilarError(current, last string) bool {
	// 如果没有上一条错误消息，则不相似
	if last == "" {
		return false
	}

	// 移除时间戳、数字和特定ID等可变信息
	currentCleaned := cleanErrorMessage(current)
	lastCleaned := cleanErrorMessage(last)

	// 如果清理后的消息完全相同，认为是重复错误
	if currentCleaned == lastCleaned {
		return true
	}

	// 计算相似度
	return calculateSimilarity(currentCleaned, lastCleaned) > 0.8 // 80%相似度阈值
}

// 清理错误消息中的可变信息
func cleanErrorMessage(message string) string {
	// 移除时间戳格式
	timePattern := regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(\.\d+)?([-+]\d{2}:?\d{2}|Z)?`)
	message = timePattern.ReplaceAllString(message, "")

	// 移除十六进制ID
	hexPattern := regexp.MustCompile(`0x[0-9a-fA-F]+`)
	message = hexPattern.ReplaceAllString(message, "")

	// 移除UUID格式
	uuidPattern := regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	message = uuidPattern.ReplaceAllString(message, "")

	// 移除普通数字
	numberPattern := regexp.MustCompile(`\b\d+\b`)
	message = numberPattern.ReplaceAllString(message, "")

	// 移除多余的空白字符
	spacePattern := regexp.MustCompile(`\s+`)
	message = spacePattern.ReplaceAllString(message, " ")

	return strings.TrimSpace(message)
}

// 计算两个字符串的相似度（使用Levenshtein距离）
func calculateSimilarity(s1, s2 string) float64 {
	len1, len2 := len(s1), len(s2)
	if len1 == 0 || len2 == 0 {
		return 0.0
	}

	// 创建距离矩阵
	matrix := make([][]int, len1+1)
	for i := range matrix {
		matrix[i] = make([]int, len2+1)
		matrix[i][0] = i
	}
	for j := range matrix[0] {
		matrix[0][j] = j
	}

	// 计算Levenshtein距离
	for i := 1; i <= len1; i++ {
		for j := 1; j <= len2; j++ {
			if s1[i-1] == s2[j-1] {
				matrix[i][j] = matrix[i-1][j-1]
			} else {
				matrix[i][j] = min(matrix[i-1][j-1]+1, // 替换
					min(matrix[i][j-1]+1, // 插入
						matrix[i-1][j]+1)) // 删除
			}
		}
	}

	// 计算相似度得分（0到1之间）
	maxLen := float64(max(len1, len2))
	distance := float64(matrix[len1][len2])
	return 1.0 - (distance / maxLen)
}

// 辅助函数：返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 辅助函数：返回两个整数中的较大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 带上下文的日志事件创建函数
func toLogEventWithContext(lines []string, lineNumbers []int, filePath string, startLine int, allLines []string, contextLines int) LogEvent {
	host, _ := os.Hostname()
	text := strings.Join(lines, "\n")
	tags := extractTags(lines)
	score := calculateSeverityScore(lines, tags)
	eventID := extractEventID(lines)
	timestamp := extractTimestamp(lines) // 智能提取时间戳

	// 提取上下文行
	contextBefore, contextAfter := extractContext(allLines, startLine-1, contextLines)
	contextLinesResult := append(contextBefore, contextAfter...)

	return LogEvent{
		RawLines:      lines,
		RawText:       text,
		Timestamp:     timestamp,
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

	// 包含多个错误关键词加分
	errorCount := 0
	for _, kw := range []string{"ERROR", "EXCEPTION", "FAILED", "FATAL"} {
		if strings.Contains(upperText, kw) {
			errorCount++
		}
	}
	if errorCount > 1 {
		score += errorCount
	}

	// 确保最小分数为最高单项分数
	if score < maxScore {
		score = maxScore
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

// 智能提取EventID，支持多种ID格式
func extractEventID(lines []string) string {
	// ID提取模式，按优先级排序
	idPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)trace[_-]?id[:\s=]+([a-zA-Z0-9-_]+)`),
		regexp.MustCompile(`(?i)request[_-]?id[:\s=]+([a-zA-Z0-9-_]+)`),
		regexp.MustCompile(`(?i)session[_-]?id[:\s=]+([a-zA-Z0-9-_]+)`),
		regexp.MustCompile(`(?i)correlation[_-]?id[:\s=]+([a-zA-Z0-9-_]+)`),
		regexp.MustCompile(`(?i)transaction[_-]?id[:\s=]+([a-zA-Z0-9-_]+)`),
		regexp.MustCompile(`(?i)\[([a-zA-Z0-9-_]{8,})\]`),                                                   // [ID] 格式
		regexp.MustCompile(`([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})`), // UUID格式
	}

	for _, line := range lines {
		for _, pattern := range idPatterns {
			if matches := pattern.FindStringSubmatch(line); len(matches) > 1 {
				id := strings.TrimSpace(matches[1])
				if len(id) >= 6 { // 确保ID有足够长度
					return id
				}
			}
		}
	}

	// 如果没有找到ID，生成基于内容的哈希ID
	return generateContentBasedID(lines)
}

// 生成基于内容的唯一ID
func generateContentBasedID(lines []string) string {
	if len(lines) == 0 {
		return fmt.Sprintf("auto_%d", time.Now().UnixNano())
	}

	// 使用第一行的哈希值作为ID
	content := lines[0]
	if len(content) > 50 {
		content = content[:50] // 限制长度
	}

	// 简单哈希算法
	hash := uint32(0)
	for _, c := range content {
		hash = hash*31 + uint32(c)
	}

	return fmt.Sprintf("hash_%x_%d", hash, time.Now().Unix())
}

// 智能提取时间戳
func extractTimestamp(lines []string) string {
	// 常见的时间戳格式模式
	timestampPatterns := []*regexp.Regexp{
		// ISO 8601 格式
		regexp.MustCompile(`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?(?:Z|[+-]\d{2}:\d{2})?)`),
		// 标准日期时间格式
		regexp.MustCompile(`(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)`),
		// 日志常见格式
		regexp.MustCompile(`(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})`),
		// 另一种常见格式
		regexp.MustCompile(`(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})`),
		// 带毫秒的格式
		regexp.MustCompile(`(\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3})`),
		// Unix时间戳（10位或13位）
		regexp.MustCompile(`\b(1[0-9]{9,12})\b`),
	}

	// 对应的时间格式
	timeFormats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05.000",
		"01/02/2006 15:04:05",
		"2006/01/02 15:04:05",
		"01-02-2006 15:04:05.000",
		"", // Unix时间戳特殊处理
	}

	for _, line := range lines {
		for i, pattern := range timestampPatterns {
			if matches := pattern.FindStringSubmatch(line); len(matches) > 1 {
				timestampStr := matches[1]

				// 特殊处理Unix时间戳
				if i == len(timestampPatterns)-1 {
					if timestamp, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
						// 判断是秒还是毫秒
						if timestamp > 1000000000000 { // 13位，毫秒
							return time.Unix(timestamp/1000, (timestamp%1000)*1000000).Format(time.RFC3339)
						} else { // 10位，秒
							return time.Unix(timestamp, 0).Format(time.RFC3339)
						}
					}
					continue
				}

				// 尝试解析时间格式
				format := timeFormats[i]
				if format == "2006-01-02 15:04:05.000" {
					// 处理可能没有毫秒的情况
					if !strings.Contains(timestampStr, ".") {
						format = "2006-01-02 15:04:05"
					}
				}

				if parsedTime, err := time.Parse(format, timestampStr); err == nil {
					return parsedTime.Format(time.RFC3339)
				}
			}
		}
	}

	// 如果没有找到时间戳，使用当前时间
	return time.Now().Format(time.RFC3339)
}
