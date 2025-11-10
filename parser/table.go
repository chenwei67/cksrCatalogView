/*
 * @File : table
 * @Date : 2025/7/9 15:02
 * @Author : Tangshiyu tshiyuoo@gmail.com
 * @Version: 1.0.0
 * @Description:
 */

package parser

import (
    "strings"
    "cksr/logger"
)

func (t *Table) parserTableName(s string) bool {
	indexStr := "CREATE TABLE"
	if !strings.HasPrefix(s, indexStr) {
		return false
	}

	logger.Debug("解析CREATE TABLE行: %s", s)

	// 移除CREATE TABLE前缀
	remaining := strings.TrimSpace(s[len(indexStr):])
	logger.Debug("移除CREATE TABLE后: %s", remaining)

	// 找到表名部分（在第一个空格或括号之前）
	var tableName string

	// 处理带反引号的表名
	if strings.HasPrefix(remaining, "`") {
		// 找到第二个反引号的位置
		endQuote := strings.Index(remaining[1:], "`")
		if endQuote != -1 {
			tableName = remaining[1 : endQuote+1] // 不包含反引号
		}
	} else {
		// 处理不带反引号的表名
		parts := strings.Fields(remaining)
		if len(parts) > 0 {
			tableName = parts[0]
			// 移除可能的括号
			if strings.Contains(tableName, "(") {
				tableName = strings.Split(tableName, "(")[0]
			}
		}
	}

	logger.Debug("提取的表名: %s", tableName)

	if tableName == "" {
		logger.Debug("表名解析失败")
		return false
	}
	// 处理数据库名.表名的格式
	if strings.Contains(tableName, ".") {
		names := strings.Split(tableName, ".")
		if len(names) >= 2 {
			t.DDL.DBName = strings.Trim(names[0], "`")
			t.DDL.TableName = strings.Trim(names[1], "`")
		}
	} else {
		t.DDL.TableName = strings.Trim(tableName, "`")
	}
	logger.Debug("最终解析结果 - 数据库: %s, 表名: %s", t.DDL.DBName, t.DDL.TableName)
	return true
}

func fetchWord(ss string, begin int) (string, int) {
	// 跳过开头的空格
	for ; begin < len(ss); begin++ {
		if ss[begin] == ' ' {
			continue
		}
		break
	}

	s := []rune(ss)
	var word string
	var stack = make([]rune, 100)
	var stackIndex = 1
	inQuotes := false // 添加引号状态跟踪

	for ; begin <= len(s)-1; begin++ {
		if begin == len(s)-1 && s[begin] == ',' {
			break
		}
		// 关键修复：只有在非引号内才因空格退出
		if s[begin] == ' ' && stackIndex <= 1 && !inQuotes {
			break
		}

		word += string(s[begin])

		switch s[begin] {
		case '"': // 处理双引号
			inQuotes = !inQuotes
		case 39: // 处理单引号
			if stack[stackIndex-1] == 39 {
				stackIndex--
			} else {
				stack[stackIndex] = 39
				stackIndex++
			}
		case '[':
			stack[stackIndex] = '['
			stackIndex++
		case ']':
			if stack[stackIndex-1] == '[' {
				stackIndex--
			}
		case '(':
			stack[stackIndex] = s[begin]
			stackIndex++
		case ')':
			if stack[stackIndex-1] == '(' {
				stackIndex--
			}
		case '<':
			stack[stackIndex] = s[begin]
			stackIndex++
		case '>':
			if stack[stackIndex-1] == '<' {
				stackIndex--
			}
		}
	}
	if stackIndex > 1 {
		logger.Error("字段解析异常,括号退栈失败, %s | %d %d", ss, len(stack), stackIndex)
		panic("字段解析异常,括号退栈失败")
	}
	return word, begin
}

func (t *Table) parserField(s string) bool {
    indexStr := "`"
    if !strings.HasPrefix(s, indexStr) {
        return false
    }
    var fd = Field{}
    var index int
    fd.Name, index = fetchWord(s, index)
    fd.Type, index = fetchWord(s, index)
    fd.Name = strings.ReplaceAll(fd.Name, "`", "")

    // 仅解析 DEFAULT 默认值（CK/SR）
    for {
        tok, next := fetchWord(s, index)
        tokTrim := strings.TrimSpace(tok)
        if tokTrim == "" {
            break
        }
        upper := strings.ToUpper(tokTrim)

        switch upper {
        case "DEFAULT":
            val, next2 := fetchWord(s, next)
            fd.DefaultKind = "DEFAULT"
            fd.DefaultExpr = strings.TrimSpace(val)
            index = next2
            continue
        }

        // 推进index到下一个位置
        index = next
    }

    t.Field = append(t.Field, fd)
    return true
}


func (t *Table) parserTTL(s string) bool {
	indexStr := "ttl "
	tmpStr := strings.ToLower(s)
	if strings.HasPrefix(tmpStr, indexStr) {
		t.DDL.TTL = s
		return true
	}
	ttlRel := fetchValFromFunc(t.DDL.TTL)
	if len(ttlRel) > 0 {
		t.DDL.TTL = ttlRel
	}
	return false
}
