package builder

import (
	"cksr/logger"
	"fmt"
	"strings"
)

type CKAddColumnBuilder struct {
	converter FieldConverter
}

func NewCKAddColumnBuilder(c FieldConverter) CKAddColumnBuilder {
	return CKAddColumnBuilder{converter: c}
}

func (c CKAddColumnBuilder) Build() string {
	logger.Debug("CKAddColumnBuilder.Build() 开始构建字段: %s", c.converter.Name)

	if c.converter.OriginField == nil {
		logger.Debug("CKAddColumnBuilder.Build() 原始字段为空，跳过字段: %s", c.converter.Name)
		return ""
	}

	_type := c.converter.OriginField.Type
	logger.Debug("CKAddColumnBuilder.Build() 字段 %s 的类型: %s", c.converter.Name, _type)

	if IsArrayIPV6(_type) {
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 识别为Array(IPv6)类型，使用 stringAlias + aliasArrayIPV6StringConcat", c.converter.Name)
		result := c.buildAddLine(c.stringAlias, c.aliasArrayIPV6StringConcat)
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 构建结果: %s", c.converter.Name, result)
		return result
	} else if IsStringArray(_type) {
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 识别为字符串数组类型，使用 stringAlias + aliasStringConcat", c.converter.Name)
		result := c.buildAddLine(c.stringAlias, c.aliasStringConcat)
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 构建结果: %s", c.converter.Name, result)
		return result
	} else if IsArray(_type) {
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 识别为数组类型，使用 stringAlias + aliasMapStringConcat", c.converter.Name)
		result := c.buildAddLine(c.stringAlias, c.aliasMapStringConcat)
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 构建结果: %s", c.converter.Name, result)
		return result
	} else if IsIPV6(_type) {
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 识别为IPv6类型，使用 uint128Alias + aliasReinterpretAsUInt128", c.converter.Name)
		result := c.buildAddLine(c.uint128Alias, c.aliasReinterpretAsUInt128)
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 构建结果: %s", c.converter.Name, result)
		return result
	} else if IsIPV4(_type) {
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 识别为IPv4类型，使用 uint32Alias + aliasToUInt32", c.converter.Name)
		result := c.buildAddLine(c.uint32Alias, c.aliasToUInt32)
		logger.Debug("CKAddColumnBuilder.Build() 字段 %s 构建结果: %s", c.converter.Name, result)
		return result
	}

	// 不需要新增列
	logger.Debug("CKAddColumnBuilder.Build() 字段 %s 不需要新增列，跳过", c.converter.Name)
	return ""
}

func (c CKAddColumnBuilder) buildAddLine(aliasFunc func(string, string) string, transFunc func(string) string) string {
	logger.Debug("CKAddColumnBuilder.buildAddLine() 开始构建字段 %s 的ADD LINE", c.converter.Name)

	originalFieldName := c.converter.OriginField.Name
	logger.Debug("CKAddColumnBuilder.buildAddLine() 原始字段名: %s", originalFieldName)

	transformedName := transFunc(originalFieldName)
	logger.Debug("CKAddColumnBuilder.buildAddLine() 转换后的表达式: %s", transformedName)

	aliasExpression := aliasFunc(c.converter.Name, transformedName)
	logger.Debug("CKAddColumnBuilder.buildAddLine() 别名表达式: %s", aliasExpression)

	result := c.addColumn(aliasExpression)
	logger.Debug("CKAddColumnBuilder.buildAddLine() 最终ADD COLUMN语句: %s", result)

	return result
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
	return fmt.Sprintf("reinterpretAsUInt128(reverse(reinterpretAsFixedString(%s))) - - toUInt128('170141183460469231731687303715884105728')", name)
}

func (c CKAddColumnBuilder) aliasArrayIPV6StringConcat(arrayName string) string {
	return fmt.Sprintf("arrayStringConcat(arrayMap(x -> reinterpretAsUInt128(reverse(reinterpretAsFixedString(x))) - toUInt128('170141183460469231731687303715884105728'), %s), 'CKTOSRFRAGEMENT')", arrayName)
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
	logger.Debug("CKAddColumnsBuilder.Build() 开始构建ALTER TABLE语句")
	logger.Debug("CKAddColumnsBuilder.Build() 数据库: %s, 表名: %s", c.dbName, c.tableName)
	logger.Debug("CKAddColumnsBuilder.Build() 字段构建器数量: %d", len(c.builders))

	// 如果没有需要添加的字段，返回空字符串
	if len(c.builders) == 0 {
		logger.Debug("CKAddColumnsBuilder.Build() 没有字段构建器，返回空字符串")
		return ""
	}

	// 收集所有有效的字段定义
	var validFields []string
	logger.Debug("CKAddColumnsBuilder.Build() 开始收集有效字段定义...")

	for i, builder := range c.builders {
		logger.Debug("CKAddColumnsBuilder.Build() 处理第 %d 个字段构建器", i+1)
		s := builder.Build()
		// 跳过空的字段定义
		if s != "" {
			logger.Debug("CKAddColumnsBuilder.Build() 添加有效字段定义: %s", s)
			validFields = append(validFields, s)
		} else {
			logger.Debug("CKAddColumnsBuilder.Build() 跳过空字段定义")
		}
	}

	logger.Debug("CKAddColumnsBuilder.Build() 收集到 %d 个有效字段定义", len(validFields))

	// 如果没有有效字段，返回空字符串
	if len(validFields) == 0 {
		logger.Debug("CKAddColumnsBuilder.Build() 没有有效字段定义，返回空字符串")
		return ""
	}

	// 构建单条ALTER TABLE语句，包含多个ADD COLUMN子句
	logger.Debug("CKAddColumnsBuilder.Build() 开始构建最终ALTER TABLE语句...")
	var result strings.Builder

	alterTableHeader := fmt.Sprintf("ALTER TABLE %s.%s on cluster '{cluster}'\n", c.dbName, c.tableName)
	logger.Debug("CKAddColumnsBuilder.Build() ALTER TABLE头部: %s", strings.TrimSpace(alterTableHeader))
	result.WriteString(alterTableHeader)

	for i, field := range validFields {
		if i == len(validFields)-1 {
			// 最后一个字段后面加分号结束
			fieldLine := fmt.Sprintf("%s;", field)
			logger.Debug("CKAddColumnsBuilder.Build() 添加最后一个字段 (第%d个): %s", i+1, fieldLine)
			result.WriteString(fieldLine)
		} else {
			// 非最后一个字段后面加逗号和换行
			fieldLine := fmt.Sprintf("%s,\n", field)
			logger.Debug("CKAddColumnsBuilder.Build() 添加字段 (第%d个): %s", i+1, strings.TrimSpace(fieldLine))
			result.WriteString(fieldLine)
		}
	}

	finalSQL := result.String()
	logger.Debug("CKAddColumnsBuilder.Build() 构建完成，最终SQL长度: %d 字符", len(finalSQL))
	logger.Debug("CKAddColumnsBuilder.Build() 最终SQL语句:\n%s", finalSQL)

	return finalSQL
}

// BuildWithExistenceCheck 构建带字段存在性检查的ALTER TABLE语句
// 这个方法与Build()方法功能相同，但提供了更明确的语义
func (c CKAddColumnsBuilder) BuildWithExistenceCheck() string {
	return c.Build()
}
