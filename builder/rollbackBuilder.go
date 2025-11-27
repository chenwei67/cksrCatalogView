package builder

import (
	"fmt"
	"strings"

	"cksr/logger"
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
