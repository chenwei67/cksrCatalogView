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
	return fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s", s)
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
	// 如果没有需要添加的字段，返回空字符串
	if len(c.builders) == 0 {
		return ""
	}
	
	// 收集所有有效的字段定义
	var validFields []string
	for _, builder := range c.builders {
		s := builder.Build()
		// 跳过空的字段定义
		if s != "" {
			validFields = append(validFields, s)
		}
	}
	
	// 如果没有有效字段，返回空字符串
	if len(validFields) == 0 {
		return ""
	}
	
	// 构建ALTER TABLE语句，每个字段单独一条语句以支持IF NOT EXISTS
	var statements []string
	for _, field := range validFields {
		stmt := fmt.Sprintf("ALTER TABLE %s.%s %s;", c.dbName, c.tableName, field)
		statements = append(statements, stmt)
	}
	
	// 将所有语句用换行符连接
	var result string
	for i, stmt := range statements {
		if i == len(statements)-1 {
			result += stmt
		} else {
			result += stmt + "\n"
		}
	}
	
	return result
}

// BuildWithExistenceCheck 构建带字段存在性检查的ALTER TABLE语句
// 这个方法与Build()方法功能相同，但提供了更明确的语义
func (c CKAddColumnsBuilder) BuildWithExistenceCheck() string {
	return c.Build()
}
