package builder

import (
	"fmt"
	"strings"

	"cksr/logger"
	"cksr/parser"
)

// RollbackBuilder 回退功能构建器
type RollbackBuilder struct {
	dbName    string
	tableName string
}

// NewRollbackBuilder 创建回退构建器
func NewRollbackBuilder(dbName, tableName string) RollbackBuilder {
	return RollbackBuilder{
		dbName:    dbName,
		tableName: tableName,
	}
}

// BuildDropViewSQL 构建删除视图的SQL
func (r RollbackBuilder) BuildDropViewSQL() string {
	logger.Debug("RollbackBuilder.BuildDropViewSQL() 构建删除视图SQL: %s.%s", r.dbName, r.tableName)
	sql := fmt.Sprintf("DROP VIEW IF EXISTS `%s`.`%s`", r.dbName, r.tableName)
	logger.Debug("RollbackBuilder.BuildDropViewSQL() 生成SQL: %s", sql)
	return sql
}

// BuildDropCatalogSQL 构建删除Catalog的SQL
func (r RollbackBuilder) BuildDropCatalogSQL(catalogName string) string {
	logger.Debug("RollbackBuilder.BuildDropCatalogSQL() 构建删除Catalog SQL: %s", catalogName)
	sql := fmt.Sprintf("DROP CATALOG IF EXISTS `%s`", catalogName)
	logger.Debug("RollbackBuilder.BuildDropCatalogSQL() 生成SQL: %s", sql)
	return sql
}

// BuildDropCKColumnSQL 构建删除ClickHouse表中带后缀列的SQL
func (r RollbackBuilder) BuildDropCKColumnSQL(columnName string) string {
	logger.Debug("RollbackBuilder.BuildDropCKColumnSQL() 构建删除CK列SQL: %s.%s.%s", r.dbName, r.tableName, columnName)
	sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` on cluster '{cluster}' DROP COLUMN IF EXISTS `%s`", r.dbName, r.tableName, columnName)
	logger.Debug("RollbackBuilder.BuildDropCKColumnSQL() 生成SQL: %s", sql)
	return sql
}

// BuildDropSRColumnSQL 构建删除StarRocks表中新增列的SQL
func (r RollbackBuilder) BuildDropSRColumnSQL(columnName string) string {
	logger.Debug("RollbackBuilder.BuildDropSRColumnSQL() 构建删除SR列SQL: %s.%s.%s", r.dbName, r.tableName, columnName)
	sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP COLUMN `%s`", r.dbName, r.tableName, columnName)
	logger.Debug("RollbackBuilder.BuildDropSRColumnSQL() 生成SQL: %s", sql)
	return sql
}

// BuildDropSRIndexSQL 构建删除StarRocks表中索引的SQL
func (r RollbackBuilder) BuildDropSRIndexSQL(indexName string) string {
	logger.Debug("RollbackBuilder.BuildDropSRIndexSQL() 构建删除SR索引SQL: %s.%s.%s", r.dbName, r.tableName, indexName)
	sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`", r.dbName, r.tableName, indexName)
	logger.Debug("RollbackBuilder.BuildDropSRIndexSQL() 生成SQL: %s", sql)
	return sql
}

// BuildRenameSRTableSQL 构建重命名StarRocks表的SQL（去掉后缀）
func (r RollbackBuilder) BuildRenameSRTableSQL(suffix string) string {
	if !strings.HasSuffix(r.tableName, suffix) {
		logger.Debug("RollbackBuilder.BuildRenameSRTableSQL() 表名不包含后缀，跳过: %s", r.tableName)
		return ""
	}
	
	originalTableName := strings.TrimSuffix(r.tableName, suffix)
	logger.Debug("RollbackBuilder.BuildRenameSRTableSQL() 构建重命名SQL: %s -> %s", r.tableName, originalTableName)
	sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` RENAME `%s`", r.dbName, r.tableName, originalTableName)
	logger.Debug("RollbackBuilder.BuildRenameSRTableSQL() 生成SQL: %s", sql)
	return sql
}

// RollbackColumnsBuilder 批量回退列构建器
type RollbackColumnsBuilder struct {
	builders  []RollbackBuilder
	dbName    string
	tableName string
}

// NewRollbackColumnsBuilder 创建批量回退列构建器
func NewRollbackColumnsBuilder(dbName, tableName string) RollbackColumnsBuilder {
	return RollbackColumnsBuilder{
		dbName:    dbName,
		tableName: tableName,
	}
}

// BuildDropCKColumnsSQL 构建删除ClickHouse表中所有带后缀列的SQL
func (r RollbackColumnsBuilder) BuildDropCKColumnsSQL(fields []parser.Field) []string {
	logger.Debug("RollbackColumnsBuilder.BuildDropCKColumnsSQL() 开始构建删除CK列SQL，表: %s.%s", r.dbName, r.tableName)
	
	var sqls []string
	builder := NewRollbackBuilder(r.dbName, r.tableName)
	
	for _, field := range fields {
		// 检查是否是带后缀的列（通过add column操作新增的）
		if IsAddedColumnByName(field.Name) {
			sql := builder.BuildDropCKColumnSQL(field.Name)
			if sql != "" {
				sqls = append(sqls, sql)
			}
		}
	}
	
	logger.Debug("RollbackColumnsBuilder.BuildDropCKColumnsSQL() 生成%d条删除CK列SQL", len(sqls))
	return sqls
}

// BuildDropSRColumnsSQL 构建删除StarRocks表中所有新增列的SQL
func (r RollbackColumnsBuilder) BuildDropSRColumnsSQL(fields []parser.Field) []string {
	logger.Debug("RollbackColumnsBuilder.BuildDropSRColumnsSQL() 开始构建删除SR列SQL，表: %s.%s", r.dbName, r.tableName)
	
	var sqls []string
	builder := NewRollbackBuilder(r.dbName, r.tableName)
	
	for _, field := range fields {
		// 检查是否是新增的列（这里需要根据实际情况判断哪些是新增的列）
		// 可以通过列名模式、创建时间或其他标识来判断
		if r.isAddedSRColumn(field) {
			sql := builder.BuildDropSRColumnSQL(field.Name)
			if sql != "" {
				sqls = append(sqls, sql)
			}
		}
	}
	
	logger.Debug("RollbackColumnsBuilder.BuildDropSRColumnsSQL() 生成%d条删除SR列SQL", len(sqls))
	return sqls
}

// isAddedSRColumn 判断是否是新增的SR列
// 这里可以根据实际需求来实现判断逻辑
func (r RollbackColumnsBuilder) isAddedSRColumn(field parser.Field) bool {
	// 示例：如果列名包含特定后缀或模式，则认为是新增的列
	// 这里需要根据实际的列命名规则来调整
	return strings.Contains(field.Name, "sync_from_ck") || 
		   strings.Contains(field.Name, "_added") ||
		   strings.Contains(field.Name, "_new")
}

// ViewRollbackBuilder 视图回退构建器
type ViewRollbackBuilder struct {
	viewName string
	dbName   string
}

// NewViewRollbackBuilder 创建视图回退构建器
func NewViewRollbackBuilder(viewName, dbName string) ViewRollbackBuilder {
	return ViewRollbackBuilder{
		viewName: viewName,
		dbName:   dbName,
	}
}

// BuildDropAllViewsSQL 构建删除所有视图的SQL
func (v ViewRollbackBuilder) BuildDropAllViewsSQL(viewNames []string) []string {
	logger.Debug("ViewRollbackBuilder.BuildDropAllViewsSQL() 开始构建删除所有视图SQL，数据库: %s", v.dbName)
	
	var sqls []string
	for _, viewName := range viewNames {
		builder := NewRollbackBuilder(v.dbName, viewName)
		sql := builder.BuildDropViewSQL()
		if sql != "" {
			sqls = append(sqls, sql)
		}
	}
	
	logger.Debug("ViewRollbackBuilder.BuildDropAllViewsSQL() 生成%d条删除视图SQL", len(sqls))
	return sqls
}

// TableRollbackBuilder 表回退构建器
type TableRollbackBuilder struct {
	dbName string
}

// NewTableRollbackBuilder 创建表回退构建器
func NewTableRollbackBuilder(dbName string) TableRollbackBuilder {
	return TableRollbackBuilder{
		dbName: dbName,
	}
}

// BuildRenameSRTablesSQL 构建重命名所有StarRocks表的SQL（去掉后缀）
func (t TableRollbackBuilder) BuildRenameSRTablesSQL(tableNames []string, suffix string) []string {
	logger.Debug("TableRollbackBuilder.BuildRenameSRTablesSQL() 开始构建重命名SR表SQL，数据库: %s，后缀: %s", t.dbName, suffix)
	
	var sqls []string
	for _, tableName := range tableNames {
		builder := NewRollbackBuilder(t.dbName, tableName)
		sql := builder.BuildRenameSRTableSQL(suffix)
		if sql != "" {
			sqls = append(sqls, sql)
		}
	}
	
	logger.Debug("TableRollbackBuilder.BuildRenameSRTablesSQL() 生成%d条重命名表SQL", len(sqls))
	return sqls
}