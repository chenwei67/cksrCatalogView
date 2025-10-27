package main

import (
	"fmt"
	"strings"

	"cksr/builder"
	"cksr/config"
	"cksr/database"
	"cksr/logger"
)

// RollbackManager 回退管理器
type RollbackManager struct {
	dbManager *database.DatabasePairManager
	cfg       *config.Config
	pair      config.DatabasePair
}

// NewRollbackManager 创建回退管理器
func NewRollbackManager(cfg *config.Config, pairIndex int) *RollbackManager {
	return &RollbackManager{
		dbManager: database.NewDatabasePairManager(cfg, pairIndex),
		cfg:       cfg,
		pair:      cfg.DatabasePairs[pairIndex],
	}
}

// ExecuteRollback 执行完整的回退操作
func (rm *RollbackManager) ExecuteRollback() error {
	logger.Info("开始执行回退操作，数据库对: %s", rm.pair.Name)

	// 1. 删除所有视图
	if err := rm.dropAllViews(); err != nil {
		return fmt.Errorf("删除视图失败: %w", err)
	}

	// 2. 删除Catalog
	if err := rm.dropCatalog(); err != nil {
		return fmt.Errorf("删除Catalog失败: %v", err)
}
	// 3. 去掉SR表的后缀
	if err := rm.removeSRTableSuffix(); err != nil {
		return fmt.Errorf("去掉SR表后缀失败: %w", err)
	}

	// 4. 删除CK表的带后缀列
	if err := rm.dropCKAddedColumns(); err != nil {
		return fmt.Errorf("删除CK表带后缀列失败: %w", err)
	}

	logger.Info("回退操作完成，数据库对: %s", rm.pair.Name)
	return nil
}

// dropCatalog 删除StarRocks中的Catalog
func (rm *RollbackManager) dropCatalog() error {
	logger.Info("正在删除StarRocks Catalog...")

	catalogName := rm.pair.CatalogName
	if catalogName == "" {
		// 如果没有配置catalog名称，使用默认格式
		return fmt.Errorf("catalog 为空，无法删除")
	}

	// 构建删除Catalog的SQL
	rollbackBuilder := builder.NewRollbackBuilder("", "")
	dropCatalogSQL := rollbackBuilder.BuildDropCatalogSQL(catalogName)

	logger.Info("正在删除Catalog: %s", catalogName)
	logger.Debug("删除Catalog SQL: %s", dropCatalogSQL)

	// 执行删除Catalog的SQL
	if err := rm.dbManager.ExecuteRollbackSQL([]string{dropCatalogSQL}, false); err != nil {
		return fmt.Errorf("执行删除Catalog SQL失败: %w", err)
	}

	logger.Info("成功删除Catalog: %s", catalogName)
	return nil
}

// dropAllViews 删除StarRocks中的所有视图
func (rm *RollbackManager) dropAllViews() error {
	logger.Info("正在删除StarRocks视图...")

	srTableNames, err := rm.dbManager.GetStarRocksTableNames()
	if err != nil {
		return fmt.Errorf("获取StarRocks表列表失败: %w", err)
	}

	var srViewNames []string
	for _, tableName := range srTableNames {
		isView, err := rm.dbManager.CheckStarRocksTableIsView(tableName)
		if err != nil {
			logger.Warn("检查StarRocks表 %s 类型失败: %v，跳过", tableName, err)
			continue
		}
		if isView {
			srViewNames = append(srViewNames, tableName)
		}
	}

	if len(srViewNames) > 0 {
		logger.Info("找到 %d 个StarRocks视图需要删除", len(srViewNames))
		srViewBuilder := builder.NewViewRollbackBuilder("", rm.pair.StarRocks.Database)
		dropSRViewSQLs := srViewBuilder.BuildDropAllViewsSQL(srViewNames)
		if err := rm.dbManager.ExecuteRollbackSQL(dropSRViewSQLs, false); err != nil {
			return fmt.Errorf("执行删除StarRocks视图SQL失败: %w", err)
		}
		logger.Info("成功删除 %d 个StarRocks视图", len(srViewNames))
	} else {
		logger.Info("没有找到需要删除的StarRocks视图")
	}

	return nil
}

// removeSRTableSuffix 去掉StarRocks表的后缀
func (rm *RollbackManager) removeSRTableSuffix() error {
	logger.Info("正在去掉StarRocks表的后缀...")

	suffix := rm.pair.SRTableSuffix
	if suffix == "" {
		logger.Info("配置中没有设置表后缀，跳过重命名操作")
		return nil
	}

	// 获取所有表名
	tableNames, err := rm.dbManager.GetStarRocksTableNames()
	if err != nil {
		return fmt.Errorf("获取StarRocks表列表失败: %w", err)
	}

	// 过滤出带有后缀的表
	var tablesWithSuffix []string
	var conflictTables []string // 记录可能冲突的表名

	for _, tableName := range tableNames {
		if strings.HasSuffix(tableName, suffix) {
			// 检查表是否为VIEW
			isView, err := rm.dbManager.CheckStarRocksTableIsView(tableName)
			if err != nil {
				logger.Warn("检查表 %s 类型失败: %v，跳过", tableName, err)
				continue
			}

			if isView {
				logger.Debug("跳过VIEW表: %s", tableName)
				continue
			}

			// 检查表是否为native表
			isNative, err := rm.dbManager.CheckStarRocksTableIsNative(tableName)
			if err != nil {
				logger.Warn("检查表 %s 类型失败: %v，跳过", tableName, err)
				continue
			}

			if !isNative {
				logger.Debug("跳过非native表: %s", tableName)
				continue
			}

			tablesWithSuffix = append(tablesWithSuffix, tableName)

			// 检查去掉后缀后的表名是否已存在
			originalTableName := strings.TrimSuffix(tableName, suffix)
			for _, existingTable := range tableNames {
				if existingTable == originalTableName {
					conflictTables = append(conflictTables, originalTableName)
					break
				}
			}
		}
	}

	if len(tablesWithSuffix) == 0 {
		logger.Info("没有找到需要去掉后缀的表")
		return nil
	}

	logger.Info("找到 %d 个表需要去掉后缀 '%s'", len(tablesWithSuffix), suffix)

	// 如果有冲突的表，先删除它们
	if len(conflictTables) > 0 {
		return fmt.Errorf("发现表名冲突，无法执行回退操作。冲突的表: %v。请手动处理这些冲突后再执行回退", conflictTables)
	}

	// 构建重命名表的SQL
	tableBuilder := builder.NewTableRollbackBuilder(rm.pair.StarRocks.Database)
	renameSQLs := tableBuilder.BuildRenameSRTablesSQL(tablesWithSuffix, suffix)

	// 执行重命名表的SQL
	if err := rm.dbManager.ExecuteRollbackSQL(renameSQLs, false); err != nil {
		return fmt.Errorf("执行重命名表SQL失败: %w", err)
	}

	logger.Info("成功去掉 %d 个表的后缀", len(tablesWithSuffix))
	return nil
}

// dropCKAddedColumns 删除ClickHouse表的带后缀列
func (rm *RollbackManager) dropCKAddedColumns() error {
	logger.Info("正在删除ClickHouse表的带后缀列...")

	// 获取ClickHouse表结构
	ckSchemaMap, err := rm.dbManager.ExportClickHouseTables()
	if err != nil {
		return fmt.Errorf("导出ClickHouse表结构失败: %w", err)
	}

	var totalDroppedColumns int

	for tableName, ddl := range ckSchemaMap {
		// 解析表结构
		table, err := parseTableFromString(ddl, rm.pair.ClickHouse.Database, tableName)
		if err != nil {
			logger.Warn("解析表 %s 结构失败: %v，跳过", tableName, err)
			continue
		}

		// 构建删除列的SQL
		var dropColumnSQLs []string
		rollbackBuilder := builder.NewRollbackBuilder(rm.pair.ClickHouse.Database, tableName)

		for _, field := range table.Field {
			// 检查是否是带后缀的列（通过add column操作新增的）
			if builder.IsAddedColumnByName(field.Name) {
				sql := rollbackBuilder.BuildDropCKColumnSQL(field.Name)
				if sql != "" {
					dropColumnSQLs = append(dropColumnSQLs, sql)
				}
			}
		}

		// 执行删除列的SQL
		if len(dropColumnSQLs) > 0 {
			logger.Debug("表 %s 需要删除 %d 个带后缀的列", tableName, len(dropColumnSQLs))
			if err := rm.dbManager.ExecuteRollbackSQL(dropColumnSQLs, true); err != nil {
				logger.Error("删除表 %s 的带后缀列失败: %v", tableName, err)
				continue
			}
			totalDroppedColumns += len(dropColumnSQLs)
		}
	}

	logger.Info("成功删除 %d 个ClickHouse表的带后缀列", totalDroppedColumns)
	return nil
}

// ExecuteRollbackForAllPairs 对所有数据库对执行回退操作
func ExecuteRollbackForAllPairs(cfg *config.Config) error {
	logger.Info("开始对所有数据库对执行回退操作...")

	for i, pair := range cfg.DatabasePairs {
		logger.Info("正在处理数据库对 %s (索引: %d)", pair.Name, i)

		rollbackManager := NewRollbackManager(cfg, i)
		if err := rollbackManager.ExecuteRollback(); err != nil {
			logger.Error("数据库对 %s 回退失败: %v", pair.Name, err)
			return fmt.Errorf("数据库对 %s 回退失败: %w", pair.Name, err)
		}

		logger.Info("数据库对 %s 回退完成", pair.Name)
	}

	logger.Info("所有数据库对回退操作完成")
	return nil
}
