package collector

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

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

// extractTags extracts tags from log lines
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

// calculateSeverityScore calculates the severity score for log events
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

// extractContext extracts context lines around the matched line
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

// ExtractEventID extracts TraceID or RequestID from log lines
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

// removeTimestamps removes timestamps from log content
func removeTimestamps(content string) string {
	// 移除常见的时间戳格式
	re := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.\d]*[+-]\d{2}:\d{2}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[,.]\d{3}|\w{3} \d{1,2} \d{2}:\d{2}:\d{2}`)
	content = re.ReplaceAllString(content, "")
	return content
}

// generateStableID generates a stable ID based on content
func generateStableID(content string) string {
	// 使用MD5哈希生成稳定的ID
	// 首先标准化内容，移除变化的部分
	normalized := normalizeContent(content)

	// 计算MD5哈希
	hash := md5.Sum([]byte(normalized))
	return hex.EncodeToString(hash[:])
}

// normalizeContent normalizes content by removing variable parts
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
