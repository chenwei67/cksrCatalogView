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
	indexes    []SRIndexDef
}

// SRColumnDef StarRocks字段定义
type SRColumnDef struct {
	Name         string
	Type         string
	DefaultValue string
	Comment      string
}

// SRIndexDef StarRocks索引定义
type SRIndexDef struct {
	IndexName  string
	ColumnName string
	IndexType  string // "INDEX" 或 "UNIQUE INDEX"
}

// NewSRAddColumnBuilder 创建新的StarRocks添加字段构建器
func NewSRAddColumnBuilder(database, tableName string) *SRAddColumnBuilder {
	return &SRAddColumnBuilder{
		database:  database,
		tableName: tableName,
		columns:   make([]SRColumnDef, 0),
		indexes:   make([]SRIndexDef, 0),
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

// AddIndex 添加索引定义
func (s *SRAddColumnBuilder) AddIndex(indexName, columnName, indexType string) *SRAddColumnBuilder {
	s.indexes = append(s.indexes, SRIndexDef{
		IndexName:  indexName,
		ColumnName: columnName,
		IndexType:  indexType,
	})
	return s
}

// AddSyncFromCKColumn 添加syncFromCK字段并创建索引
func (s *SRAddColumnBuilder) AddSyncFromCKColumn() *SRAddColumnBuilder {
	s.AddColumn("syncFromCK", "BOOLEAN", "'false'", "标识数据是否来自ClickHouse同步")
	// 为syncFromCK列添加普通索引，提高查询性能
	s.AddIndex("idx_syncFromCK", "syncFromCK", "INDEX")
	return s
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

// BuildIndexes 构建CREATE INDEX语句
func (s *SRAddColumnBuilder) BuildIndexes() []string {
	if len(s.indexes) == 0 {
		return nil
	}

	var indexSQLs []string
	for _, idx := range s.indexes {
		var sql string
		if idx.IndexType == "UNIQUE INDEX" {
			sql = fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s`.`%s` (`%s`)",
				idx.IndexName, s.database, s.tableName, idx.ColumnName)
		} else {
			sql = fmt.Sprintf("CREATE INDEX `%s` ON `%s`.`%s` (`%s`)",
				idx.IndexName, s.database, s.tableName, idx.ColumnName)
		}
		indexSQLs = append(indexSQLs, sql)
	}

	return indexSQLs
}

// BuildWithIndexes 构建包含字段和索引的完整SQL语句集合
func (s *SRAddColumnBuilder) BuildWithIndexes() []string {
	var sqls []string
	
	// 添加字段的SQL
	columnSQL := s.Build()
	if columnSQL != "" {
		sqls = append(sqls, columnSQL)
	}
	
	// 添加索引的SQL
	indexSQLs := s.BuildIndexes()
	sqls = append(sqls, indexSQLs...)
	
	return sqls
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