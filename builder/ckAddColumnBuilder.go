package builder

import (
	"fmt"
)

type CKAddColumnBuilder struct {
	converter FieldConverter
}

func NewCKAddColumnBuilder(c FieldConverter) CKAddColumnBuilder {
	return CKAddColumnBuilder{converter: c}
}

func (c CKAddColumnBuilder) Build() string {
	if c.converter.OriginField == nil {
		return ""
	}
	_type := c.converter.OriginField.Type
	if IsStringArray(_type) {
		return c.buildAddLine(c.stringAlias, c.aliasStringConcat)
	} else if IsArray(_type) {
		return c.buildAddLine(c.stringAlias, c.aliasMapStringConcat)
	} else if IsIPV6(_type) {
		return c.buildAddLine(c.uint128Alias, c.aliasReinterpretAsUInt128)
	} else if IsIPV4(_type) {
		return c.buildAddLine(c.uint32Alias, c.aliasToUInt32)
	}
	// 不需要新增列
	return ""
}

func (c CKAddColumnBuilder) buildAddLine(aliasFunc func(string, string) string, transFunc func(string) string) string {
	return c.addColumn(aliasFunc(c.converter.Name, transFunc(c.converter.OriginField.Name)))
}

func (c CKAddColumnBuilder) addColumn(s string) string {
	return fmt.Sprintf("ADD COLUMN %s", s)
}

func (c CKAddColumnBuilder) stringAlias(name, remain string) string {
	return fmt.Sprintf("%s String ALIAS %s", name, remain)
}

func (c CKAddColumnBuilder) aliasMapStringConcat(arrayName string) string {
	return fmt.Sprintf("arrayStringConcat(arrayMap(x -> toString(x), %s), 'CKTOSRFRAGEMENT')", arrayName)
}

func (c CKAddColumnBuilder) aliasStringConcat(arrayName string) string {
	return fmt.Sprintf("arrayStringConcat(%s, 'CKTOSRFRAGEMENT')", arrayName)
}

func (c CKAddColumnBuilder) uint32Alias(name, remain string) string {
	return fmt.Sprintf("%s UInt32 ALIAS %s", name, remain)
}

func (c CKAddColumnBuilder) uint128Alias(name, remain string) string {
	return fmt.Sprintf("%s UInt128 ALIAS %s", name, remain)
}

func (c CKAddColumnBuilder) aliasToUInt32(name string) string {
	return fmt.Sprintf("toUInt32(%s)", name)
}

func (c CKAddColumnBuilder) aliasReinterpretAsUInt128(name string) string {
	return fmt.Sprintf("reinterpretAsUInt128(reverse(reinterpretAsFixedString(%s)))", name)
}

type CKAddColumnsBuilder struct {
	builders  []CKAddColumnBuilder
	dbName    string // 数据库名
	tableName string // 表名
}

func NewCKAddColumnsBuilder(fieldConverters []FieldConverter, dbName string, tableName string) CKAddColumnsBuilder {
	var builders []CKAddColumnBuilder
	for _, f := range fieldConverters {
		if f.OriginField == nil {
			continue
		}
		builders = append(builders, NewCKAddColumnBuilder(f))
	}
	return CKAddColumnsBuilder{
		builders:  builders,
		dbName:    dbName,
		tableName: tableName,
	}
}

func (c CKAddColumnsBuilder) Build() string {
	var res string
	res = fmt.Sprintf("ALTER TABLE %s.%s \n", c.dbName, c.tableName)
	for i := 0; i < len(c.builders); i++ {
		s := c.builders[i].Build()
		if i != len(c.builders)-1 {
			s = fmt.Sprintf("%s, \n", s)
		} else {
			s = fmt.Sprintf("%s; \n", s)
		}
		res = fmt.Sprintf("%s%s", res, s)
	}
	// 最后加一行换行，防止执行时漏了确认的换行回车
	res = fmt.Sprintf("%s \n", res)
	return res
}
