/*
 * @File : parser
 * @Date : 2025/7/9 14:31
 * @Author : Tangshiyu tshiyuoo@gmail.com
 * @Version: 1.0.0
 * @Description:
 */

package parser

import (
	"cksr/logger"
	"strings"

	"example.com/migrationLib/parser"
)

// Re-export types from migrationLab/parser to maintain compatibility
// and avoid type mismatch errors in cksr project.
type DDL = parser.DDL
type Field = parser.Field
type Table = parser.Table

func pass(s string) bool {
	if len(s) == 0 {
		return true
	}
	if strings.HasPrefix(s, "#") || strings.HasPrefix(s, "--") {
		return true
	}
	if len(s) == 1 && (s == "(" || s == ")") {
		return true
	}
	return false
}

func fetchValFromFunc(s string) string {
	if !strings.Contains(s, "(") {
		return s
	}
	ss := []rune(s)
	var stack = make([]rune, 50)
	var stackIndex = 1
	index := strings.Index(s, "(")
	stack[stackIndex] = '('
	stackIndex++

	var newWord []rune

	for index = index + 1; index < len(ss); index++ {
		switch ss[index] {
		case '(':
			stack[stackIndex] = '('
			stackIndex++
			newWord = append(newWord, '(') // 表示有嵌套函数
			continue
		case ')':
			if stackIndex != 2 { // 剥离最外面一层
				newWord = append(newWord, ')') // 表示有嵌套函数
			}
			stackIndex--
			continue
		}
		newWord = append(newWord, ss[index])
	}
	newWorkStr := strings.TrimSpace(string(newWord))
	if strings.Contains(newWorkStr, "(") {
		return fetchValFromFunc(newWorkStr)
	}
	return newWorkStr
}

func ParserTableSQL(s string) Table {
	logger.Debug("ParserTableSQL开始执行")
	// Note: We are using the parser function from migrationLab/parser
	// assuming it has the same logic or we should delegate to it.
	// But here we keep the local implementation wrapper calling local parser logic?
	// Wait, migrationLab/parser has ParserTableSQL too?
	// If we alias Table = parser.Table, we need to make sure the methods are compatible.
	// Methods defined on types in another package cannot be added here unless we use type alias.
	// Go 1.9+ type alias: type Table = parser.Table.
	// Methods of parser.Table are available on Table.

	// However, the local implementation of ParserTableSQL constructs a Table.
	// We need to use migrationLab/parser.ParserTableSQL if available, or adapt this function.

	// Let's check migrationLab/parser/parser.go content.
	return parser.ParserTableSQL(s)
}
