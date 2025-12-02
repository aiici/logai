package collector

import (
	"strings"
	"unicode"
)

// calculateSimilarity 计算两个字符串的相似度（0-100）
func calculateSimilarity(s1, s2 string) float64 {
	// 如果两个字符串完全相同，直接返回100
	if s1 == s2 {
		return 100.0
	}

	// 转换为小写进行比较
	s1 = strings.ToLower(s1)
	s2 = strings.ToLower(s2)

	// 移除空白字符
	s1 = removeWhitespace(s1)
	s2 = removeWhitespace(s2)

	// 如果其中一个为空，则相似度为0
	if len(s1) == 0 || len(s2) == 0 {
		return 0.0
	}

	// 使用编辑距离算法计算相似度
	distance := levenshteinDistance(s1, s2)
	maxLen := max(len(s1), len(s2))
	
	if maxLen == 0 {
		return 100.0
	}
	
	// 计算相似度百分比
	similarity := (1.0 - float64(distance)/float64(maxLen)) * 100.0
	return similarity
}

// removeWhitespace 移除字符串中的所有空白字符
func removeWhitespace(s string) string {
	var result strings.Builder
	for _, r := range s {
		if !unicode.IsSpace(r) {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// levenshteinDistance 计算两个字符串的编辑距离
func levenshteinDistance(s1, s2 string) int {
	// 创建二维数组
	m, n := len(s1), len(s2)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	// 初始化边界条件
	for i := 0; i <= m; i++ {
		dp[i][0] = i
	}
	for j := 0; j <= n; j++ {
		dp[0][j] = j
	}

	// 动态规划填表
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if s1[i-1] == s2[j-1] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]) + 1
			}
		}
	}

	return dp[m][n]
}

// min 返回三个整数中的最小值
func min(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= a && b <= c {
		return b
	}
	return c
}

// max 返回两个整数中的最大值
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// isSimilarEnough 检查两个日志事件是否足够相似（基于90%的阈值）
func isSimilarEnough(event1, event2 LogEvent) bool {
	// 首先检查基本属性是否一致
	if event1.Host != event2.Host {
		return false
	}

	// 计算内容相似度
	similarity := calculateSimilarity(event1.RawText, event2.RawText)
	return similarity >= 90.0
}