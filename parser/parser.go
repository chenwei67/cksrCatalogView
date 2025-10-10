/*
 * @File : parser
 * @Date : 2025/7/9 14:31
 * @Author : Tangshiyu tshiyuoo@gmail.com
 * @Version: 1.0.0
 * @Description:
 */

package parser

import (
	"fmt"
	"strings"
)

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
	fmt.Printf("      - ParserTableSQL开始执行\n")
	var t = Table{}
	lines := strings.Split(s, "\n")
	fmt.Printf("      - DDL分割为 %d 行\n", len(lines))
	
	for index := 0; index < len(lines); index++ {
		line := strings.TrimSpace(lines[index])
		if index%10 == 0 {
			fmt.Printf("      - 正在处理第 %d/%d 行\n", index+1, len(lines))
		}
		
		if pass(line) {
			continue
		}
		if t.parserTableName(line) {
			fmt.Printf("      - 解析到表名: %s\n", t.DDL.TableName)
			continue
		}
		if t.parserField(line) {
			if len(t.Field) > 0 {
				fmt.Printf("      - 解析到字段: %s (总计 %d 个字段)\n", t.Field[len(t.Field)-1].Name, len(t.Field))
			}
			continue
		}
		if t.parserIndex(line) {
			continue
		}
		if t.parserEngine(line) {
			fmt.Printf("      - 解析到引擎: %s\n", t.DDL.Engine)
			continue
		}
		if t.parserPartitionss(line) {
			continue
		}
		if t.parserOrderBy(line) {
			continue
		}
		if t.parserTTL(line) {
			continue
		}
	}
	fmt.Printf("      - ParserTableSQL执行完成，解析到 %d 个字段\n", len(t.Field))
	return t
}
