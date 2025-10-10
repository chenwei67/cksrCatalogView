/*
 * @File : srAddColumnBuilder
 * @Date : 2025/1/20 
 * @Author : Assistant
 * @Version: 1.0.0
 * @Description: StarRocks添加字段的SQL构建器，用于添加syncFromCK字段
 */

package builder

import (
	"fmt"
	"strings"
)

// SRAddColumnBuilder StarRocks添加字段的SQL构建器
type SRAddColumnBuilder struct {
	database   string
	tableName  string
	columns    []SRColumnDef
}

// SRColumnDef StarRocks字段定义
type SRColumnDef struct {
	Name         string
	Type         string
	DefaultValue string
	Comment      string
}

// NewSRAddColumnBuilder 创建新的StarRocks添加字段构建器
func NewSRAddColumnBuilder(database, tableName string) *SRAddColumnBuilder {
	return &SRAddColumnBuilder{
		database:  database,
		tableName: tableName,
		columns:   make([]SRColumnDef, 0),
	}
}

// AddColumn 添加字段定义
func (s *SRAddColumnBuilder) AddColumn(name, dataType, defaultValue, comment string) *SRAddColumnBuilder {
	s.columns = append(s.columns, SRColumnDef{
		Name:         name,
		Type:         dataType,
		DefaultValue: defaultValue,
		Comment:      comment,
	})
	return s
}

// AddSyncFromCKColumn 添加syncFromCK字段
func (s *SRAddColumnBuilder) AddSyncFromCKColumn() *SRAddColumnBuilder {
	return s.AddColumn("syncFromCK", "BOOLEAN", "'false'", "标识数据是否来自ClickHouse同步")
}

// Build 构建ALTER TABLE ADD COLUMN语句
func (s *SRAddColumnBuilder) Build() string {
	if len(s.columns) == 0 {
		return ""
	}

	var columnDefs []string
	for _, col := range s.columns {
		var colDef strings.Builder
		colDef.WriteString(fmt.Sprintf("`%s` %s", col.Name, col.Type))
		
		if col.DefaultValue != "" {
			colDef.WriteString(fmt.Sprintf(" DEFAULT %s", col.DefaultValue))
		}
		
		if col.Comment != "" {
			colDef.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
		}
		
		columnDefs = append(columnDefs, colDef.String())
	}

	return fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN (%s)",
		s.database, s.tableName, strings.Join(columnDefs, ", "))
}

// BuildIfNotExists 构建带条件检查的ALTER TABLE ADD COLUMN语句
// 返回的SQL会先检查字段是否存在，不存在才添加
func (s *SRAddColumnBuilder) BuildIfNotExists() string {
	if len(s.columns) == 0 {
		return ""
	}

	var statements []string
	for _, col := range s.columns {
		var colDef strings.Builder
		colDef.WriteString(fmt.Sprintf("`%s` %s", col.Name, col.Type))
		
		if col.DefaultValue != "" {
			colDef.WriteString(fmt.Sprintf(" DEFAULT %s", col.DefaultValue))
		}
		
		if col.Comment != "" {
			colDef.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
		}

		// 构建条件添加语句
		stmt := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s`,
			s.database, s.tableName, colDef.String())
		statements = append(statements, stmt)
	}

	return strings.Join(statements, ";\n")
}