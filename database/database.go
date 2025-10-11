/*
 * @File : database
 * @Date : 2025/1/27
 * @Author : Assistant
 * @Version: 1.0.0
 * @Description: 数据库连接管理
 */

package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"cksr/config"
	"cksr/logger"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
)

// DatabasePairManager 数据库对管理器
type DatabasePairManager struct {
	config    *config.Config
	pairIndex int
}

// NewDatabasePairManager 创建数据库对管理器（通过索引）
func NewDatabasePairManager(cfg *config.Config, pairIndex int) *DatabasePairManager {
	return &DatabasePairManager{
		config:    cfg,
		pairIndex: pairIndex,
	}
}

// NewDatabasePairManagerByName 创建数据库对管理器（通过名称）
func NewDatabasePairManagerByName(cfg *config.Config, pairName string) *DatabasePairManager {
	for i, pair := range cfg.DatabasePairs {
		if pair.Name == pairName {
			return &DatabasePairManager{
				config:    cfg,
				pairIndex: i,
			}
		}
	}
	return nil
}

// GetClickHouseConnection 获取ClickHouse连接
func (dm *DatabasePairManager) GetClickHouseConnection() (*sql.DB, error) {
	dsn := dm.config.GetClickHouseDSNByIndex(dm.pairIndex)
	if dsn == "" {
		return nil, fmt.Errorf("无效的数据库对索引: %d", dm.pairIndex)
	}

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接ClickHouse失败: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ClickHouse连接测试失败: %w", err)
	}

	return db, nil
}

// GetStarRocksConnection 获取StarRocks连接
func (dm *DatabasePairManager) GetStarRocksConnection() (*sql.DB, error) {
	dsn := dm.config.GetStarRocksDSNByIndex(dm.pairIndex)
	if dsn == "" {
		return nil, fmt.Errorf("无效的数据库对索引: %d", dm.pairIndex)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接StarRocks失败: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("StarRocks连接测试失败: %w", err)
	}

	return db, nil
}

// ExportClickHouseTables 导出ClickHouse表结构
func (dm *DatabasePairManager) ExportClickHouseTables() (map[string]string, error) {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	query := "SHOW CREATE TABLE"
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("获取表列表失败: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		createQuery := fmt.Sprintf("%s %s", query, tableName)
		var createStatement string
		if err := db.QueryRow(createQuery).Scan(&createStatement); err != nil {
			log.Printf("获取表 %s 的创建语句失败: %v", tableName, err)
			continue
		}

		result[tableName] = createStatement
	}

	return result, nil
}

// GetStarRocksTableNames 获取StarRocks表名列表
func (dm *DatabasePairManager) GetStarRocksTableNames() ([]string, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	rows, err := db.Query(fmt.Sprintf("SHOW TABLES FROM %s", pair.StarRocks.Database))
	if err != nil {
		return nil, fmt.Errorf("获取表列表失败: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tableNames = append(tableNames, tableName)
	}

	return tableNames, nil
}

// GetStarRocksTableDDL 获取指定表的DDL语句
func (dm *DatabasePairManager) GetStarRocksTableDDL(tableName string) (string, error) {
	logger.Debug("正在连接StarRocks数据库...")
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return "", fmt.Errorf("连接StarRocks数据库失败: %w", err)
	}
	defer db.Close()
	logger.Debug("StarRocks数据库连接成功")

	// 构造查询语句
	createQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", dm.config.DatabasePairs[dm.pairIndex].StarRocks.Database, tableName)
	logger.Debug("执行查询: %s", createQuery)

	// 执行查询
	rows, err := db.Query(createQuery)
	if err != nil {
		return "", fmt.Errorf("执行查询失败: %w", err)
	}
	defer rows.Close()

	logger.Debug("查询执行完成，正在读取结果...")
	
	// 读取结果
	var table, createStatement string
	if rows.Next() {
		if err := rows.Scan(&table, &createStatement); err != nil {
			return "", fmt.Errorf("读取查询结果失败: %w", err)
		}
	}
	logger.Debug("DDL读取完成，长度: %d 字符", len(createStatement))

	return createStatement, nil
}

// ExportStarRocksTables 导出StarRocks表结构
func (dm *DatabasePairManager) ExportStarRocksTables() (map[string]string, error) {
	tableNames, err := dm.GetStarRocksTableNames()
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, tableName := range tableNames {
		// 检查表是否为VIEW
		isView, err := dm.CheckStarRocksTableIsView(tableName)
		if err != nil {
			logger.Warn("检查表 %s 类型失败: %v，跳过该表", tableName, err)
			continue
		}
		
		if isView {
			logger.Debug("跳过VIEW表: %s (VIEW表不需要导出DDL)", tableName)
			continue
		}
		
		ddl, err := dm.GetStarRocksTableDDL(tableName)
		if err != nil {
			logger.Error("获取表 %s 的DDL失败: %v", tableName, err)
			continue
		}
		result[tableName] = ddl
	}

	return result, nil
}

// ExecuteClickHouseSQL 执行ClickHouse SQL
func (dm *DatabasePairManager) ExecuteClickHouseSQL(sql string) error {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(sql)
	return err
}

// ExecuteStarRocksSQL 执行StarRocks SQL
func (dm *DatabasePairManager) ExecuteStarRocksSQL(sql string) error {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(sql)
	return err
}

// CreateStarRocksCatalog 创建StarRocks Catalog
func (dm *DatabasePairManager) CreateStarRocksCatalog(catalogName string) error {
	jdbcURI := dm.config.GetClickHouseJDBCURIByIndex(dm.pairIndex)
	if jdbcURI == "" {
		return fmt.Errorf("无效的数据库对索引: %d", dm.pairIndex)
	}

	pair := dm.config.DatabasePairs[dm.pairIndex]

	createCatalogSQL := fmt.Sprintf(`
		CREATE EXTERNAL CATALOG IF NOT EXISTS %s
		PROPERTIES (
			"type" = "jdbc",
			"user" = "%s",
			"password" = "%s",
			"jdbc_uri" = "%s",
			"driver_url" = "%s",
			"driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
		)`,
		catalogName,
		pair.ClickHouse.Username,
		pair.ClickHouse.Password,
		jdbcURI,
		dm.config.DriverURL,
	)

	return dm.ExecuteStarRocksSQL(createCatalogSQL)
}

// ExecuteBatchSQL 批量执行SQL语句
func (dm *DatabasePairManager) ExecuteBatchSQL(sqlStatements []string, isClickHouse bool) error {
	for _, sql := range sqlStatements {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}

		var err error
		if isClickHouse {
			err = dm.ExecuteClickHouseSQL(sql)
		} else {
			err = dm.ExecuteStarRocksSQL(sql)
		}

		if err != nil {
			return fmt.Errorf("执行SQL失败: %s, 错误: %w", sql, err)
		}
	}
	return nil
}

// GetPairName 获取数据库对名称
func (dm *DatabasePairManager) GetPairName() string {
	if dm.pairIndex >= len(dm.config.DatabasePairs) {
		return ""
	}
	return dm.config.DatabasePairs[dm.pairIndex].Name
}

// GetPairIndex 获取数据库对索引
func (dm *DatabasePairManager) GetPairIndex() int {
	return dm.pairIndex
}

// CheckStarRocksColumnExists 检查StarRocks表中指定字段是否存在
func (dm *DatabasePairManager) CheckStarRocksColumnExists(tableName, columnName string) (bool, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return false, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 使用information_schema查询字段是否存在
	query := `
		SELECT COUNT(*) 
		FROM information_schema.columns 
		WHERE table_schema = ? 
		AND table_name = ? 
		AND column_name = ?
	`

	var count int
	err = db.QueryRow(query, pair.StarRocks.Database, tableName, columnName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("检查字段 %s.%s.%s 是否存在失败: %w", 
			pair.StarRocks.Database, tableName, columnName, err)
	}

	return count > 0, nil
}

// CheckStarRocksTableIsNative 检查StarRocks表是否为native表
func (dm *DatabasePairManager) CheckStarRocksTableIsNative(tableName string) (bool, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return false, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 使用information_schema.tables查询表类型
	query := `
		SELECT table_type 
		FROM information_schema.tables 
		WHERE table_schema = ? 
		AND table_name = ?
	`

	var tableType string
	err = db.QueryRow(query, pair.StarRocks.Database, tableName).Scan(&tableType)
	if err != nil {
		return false, fmt.Errorf("检查表 %s.%s 类型失败: %w", 
			pair.StarRocks.Database, tableName, err)
	}

	// 检查是否为BASE TABLE（native表）
	// StarRocks中native表的table_type为'BASE TABLE'
	// 非native表（如外部表、物化视图等）会有不同的table_type
	// VIEW类型的表也不是native表
	tableTypeUpper := strings.ToUpper(tableType)
	return tableTypeUpper == "BASE TABLE", nil
}

// CheckStarRocksTableIsView 检查StarRocks表是否为VIEW
func (dm *DatabasePairManager) CheckStarRocksTableIsView(tableName string) (bool, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return false, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 使用information_schema.tables查询表类型
	query := `
		SELECT table_type 
		FROM information_schema.tables 
		WHERE table_schema = ? 
		AND table_name = ?
	`

	var tableType string
	err = db.QueryRow(query, pair.StarRocks.Database, tableName).Scan(&tableType)
	if err != nil {
		return false, fmt.Errorf("检查表 %s.%s 类型失败: %w", 
			pair.StarRocks.Database, tableName, err)
	}

	// 检查是否为VIEW
	return strings.ToUpper(tableType) == "VIEW", nil
}


// CheckClickHouseColumnExists 检查ClickHouse表中指定字段是否存在
func (dm *DatabasePairManager) CheckClickHouseColumnExists(tableName, columnName string) (bool, error) {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return false, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 使用system.columns查询字段是否存在
	query := `
		SELECT COUNT(*)
		FROM system.columns
		WHERE database = ?
		AND table = ?
		AND name = ?
	`

	var count int
	err = db.QueryRow(query, pair.ClickHouse.Database, tableName, columnName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("检查ClickHouse字段 %s.%s.%s 是否存在失败: %w",
			pair.ClickHouse.Database, tableName, columnName, err)
	}

	return count > 0, nil
}
// AddSyncFromCKColumnToTable 为指定表添加syncFromCK字段（如果不存在）
func (dm *DatabasePairManager) AddSyncFromCKColumnToTable(tableName string) error {
	// 检查字段是否已存在
	exists, err := dm.CheckStarRocksColumnExists(tableName, "syncFromCK")
	if err != nil {
		return fmt.Errorf("检查syncFromCK字段是否存在失败: %w", err)
	}

	if exists {
		logger.Debug("表 %s 的 syncFromCK 字段已存在，跳过添加", tableName)
		return nil
	}

	pair := dm.config.DatabasePairs[dm.pairIndex]
	
	// 构建添加字段的SQL
	addColumnSQL := fmt.Sprintf(
		"ALTER TABLE `%s`.`%s` ADD COLUMN `syncFromCK` BOOLEAN DEFAULT \"FALSE\" COMMENT '标识数据是否来自ClickHouse同步'",
		pair.StarRocks.Database, tableName)

	logger.Debug("正在为表 %s 添加 syncFromCK 字段...", tableName)
	
	if err := dm.ExecuteStarRocksSQL(addColumnSQL); err != nil {
		return fmt.Errorf("为表 %s 添加 syncFromCK 字段失败: %w", tableName, err)
	}

	logger.Debug("表 %s 的 syncFromCK 字段添加成功", tableName)
	return nil
}

// AddSyncFromCKColumnToAllTables 为所有StarRocks表添加syncFromCK字段
func (dm *DatabasePairManager) AddSyncFromCKColumnToAllTables() error {
	tableNames, err := dm.GetStarRocksTableNames()
	if err != nil {
		return fmt.Errorf("获取StarRocks表名列表失败: %w", err)
	}

	logger.Info("正在为 %d 个StarRocks表添加syncFromCK字段...", len(tableNames))
	
	for i, tableName := range tableNames {
		logger.Debug("[%d/%d] 处理表: %s", i+1, len(tableNames), tableName)
		
		// 检查表是否为VIEW
		isView, err := dm.CheckStarRocksTableIsView(tableName)
		if err != nil {
			logger.Warn("检查表 %s 类型失败: %v，跳过添加syncFromCK字段", tableName, err)
			continue
		}
		
		if isView {
			logger.Debug("跳过VIEW表: %s (VIEW表不需要添加syncFromCK字段)", tableName)
			continue
		}
		
		if err := dm.AddSyncFromCKColumnToTable(tableName); err != nil {
			return fmt.Errorf("为表 %s 添加syncFromCK字段失败: %w", tableName, err)
		}
	}

	return nil
}
