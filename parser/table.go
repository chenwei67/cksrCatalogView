/*
 * @File : table
 * @Date : 2025/7/9 15:02
 * @Author : Tangshiyu tshiyuoo@gmail.com
 * @Version: 1.0.0
 * @Description:
 */

package parser

import (
	"fmt"
	"strings"
)

func (t *Table) parserTableName(s string) bool {
	indexStr := "CREATE TABLE"
	if !strings.HasPrefix(s, indexStr) {
		return false
	}
	ss := strings.Split(s, " ")
	t.DDL.TableName = ss[len(ss)-1]
	if strings.Contains(t.DDL.TableName, ".") {
		names := strings.Split(t.DDL.TableName, ".")
		t.DDL.DBName = names[0]
		t.DDL.TableName = names[1]
	}
	return true
}

func fetchWord(ss string, begin int) (string, int) {
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

	for ; begin <= len(s)-1; begin++ {
		if begin == len(s)-1 && s[begin] == ',' {
			break
		}
		if s[begin] == ' ' && stackIndex <= 1 {
			break
		}
		word += string(s[begin])

		switch s[begin] {
		case 39:
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
		fmt.Println("字段解析异常,括号退栈失败,", ss, "|", len(stack), stackIndex)
		panic("字段解析异常,括号退栈失败")
	}
	return word, begin
}

func (t *Table) parserField(s string) bool {
	indexStr := "`"
	sLen := len([]rune(s))
	if !strings.HasPrefix(s, indexStr) {
		return false
	}
	var fd = Field{}
	var index int
	fd.Name, index = fetchWord(s, index)
	fd.Type, index = fetchWord(s, index)
	fd.Name = strings.ReplaceAll(fd.Name, "`", "")

	for index < sLen-1 {
		var keywork, keyVal string
		keywork, index = fetchWord(s, index)
		keyVal, index = fetchWord(s, index)
		switch strings.ToLower(strings.TrimSpace(keywork)) {
		case "default":
			fd.Default = &keyVal
		case "comment":
			fd.Comment = &keyVal
		case "alias":
			fd.IsAlias = true
			if strings.Contains(s, "rowLogAlias") {
				tmpStr := strings.ReplaceAll(keyVal, "'", "")
				tmpStr = strings.ReplaceAll(tmpStr, " ", "")
				if len(tmpStr) != 0 {
					if strings.Contains(tmpStr, "[") {
						star := strings.Index(tmpStr, "[")
						end := strings.Index(tmpStr, "]")
						slice := strings.Split(tmpStr[star+1:end], ",")
						for _, val := range slice {
							if t.FullSearchFields == nil {
								t.FullSearchFields = make(map[string]string)
							}
							t.FullSearchFields[strings.TrimSpace(val)] = ""
						}
					} else {
						tmpStr2 := fetchValFromFunc(tmpStr)
						slice := strings.Split(tmpStr2, ",")
						for _, val := range slice {
							if t.FullSearchFields == nil {
								t.FullSearchFields = make(map[string]string)
							}
							t.FullSearchFields[strings.TrimSpace(val)] = ""
						}
					}
				}

			}
		case "materialized":
			fd.IsMaterialized = true
		}
		if fd.Comment != nil && fd.Default != nil && fd.IsAlias {
			break
		}
	}
	t.Field = append(t.Field, fd)
	return true
}

func (t *Table) parserIndex(s string) bool {
	indexStr := "INDEX"
	if !strings.HasPrefix(s, indexStr) {
		return false
	}
	var index int
	var indies Index
	var indexFields string
	_, index = fetchWord(s, index) // INDEX
	indies.Name, index = fetchWord(s, index)
	indexFields, index = fetchWord(s, index)
	_, index = fetchWord(s, index) // TYPE
	indies.Type, index = fetchWord(s, index)
	indies.IF = strings.Contains(indexFields, "if(")
	if indies.IF {
		indies.Fields = append(indies.Fields, strings.Split(indexFields, ",")[1])
	} else {
		indexFields = strings.ReplaceAll(indexFields, "(", "")
		indexFields = strings.ReplaceAll(indexFields, ")", "")
		indexFields = strings.ReplaceAll(indexFields, " ", "")
		indies.Fields = strings.Split(indexFields, ",")
	}

	t.Index = append(t.Index, indies)
	return true

}

func (t *Table) parserEngine(s string) bool {
	indexStr := "engine "
	if !strings.HasPrefix(strings.ToLower(s), indexStr) {
		return false
	}
	words := strings.Split(s, "=")
	if len(words) <= 1 {
		return false
	}
	t.DDL.Engine, _ = fetchWord(words[1], 0)
	t.DDL.Engine = strings.TrimSpace(t.DDL.Engine)
	return true

}

func (t *Table) parserPartitionss(s string) bool {
	indexStr := "partition "
	indexStr2 := "by"
	tmpStr := strings.ToLower(s)
	if strings.HasPrefix(tmpStr, indexStr) && strings.Contains(tmpStr, indexStr2) {
		t.DDL.Partition = s
		return true
	}
	return false
}

func (t *Table) parserOrderBy(s string) bool {
	indexStr := "order "
	indexStr2 := "by"
	tmpStr := strings.ToLower(s)
	if !(strings.HasPrefix(tmpStr, indexStr) && strings.Contains(tmpStr, indexStr2)) {
		return false
	}

	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "ORDERBY", "")
	s = strings.ReplaceAll(s, "(", "")
	s = strings.ReplaceAll(s, ")", "")
	t.DDL.OrderBy = strings.Split(s, ",")

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
