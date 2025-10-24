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
	"strings"

	"cksr/config"
	"cksr/logger"
	"cksr/retry"

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

	// 使用重试机制获取表列表
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

		// 使用重试机制获取表的创建语句
		createQuery := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
		var createStatement string
		if err := retry.QueryRowAndScanWithRetryDefault(db, createQuery, []interface{}{&createStatement}); err != nil {
			logger.Warn("获取表 %s 的创建语句失败: %v", tableName, err)
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

	// 使用重试机制获取表列表
	rows, err := retry.QueryWithRetryDefault(db, fmt.Sprintf("SHOW TABLES FROM %s", pair.StarRocks.Database))
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

// GetStarRocksTableDDL 获取StarRocks表的DDL（使用通用重试wrapper）
func (dm *DatabasePairManager) GetStarRocksTableDDL(tableName string) (string, error) {
	var ddl string

	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return "", fmt.Errorf("获取StarRocks连接失败: %w", err)
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]
	createQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", pair.StarRocks.Database, tableName)

	err = retry.QueryRowAndScanWithRetryDefault(db, createQuery, []interface{}{&tableName, &ddl})
	if err != nil {
		return "", fmt.Errorf("获取表 %s 的DDL失败: %w", tableName, err)
	}

	return ddl, nil
}

// ExecuteClickHouseSQL 执行ClickHouse SQL
func (dm *DatabasePairManager) ExecuteClickHouseSQL(sql string) error {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	// 增加分布式 DDL 超时时间，避免 ON CLUSTER 任务因超时而提前返回错误
	if _, setErr := db.Exec("SET distributed_ddl_task_timeout = 3600"); setErr != nil {
		logger.Warn("设置 ClickHouse 分布式DDL超时失败: %v", setErr)
	}

	_, err = db.Exec(sql)
	// 如果是分布式DDL超时（错误码 159），视为非致命错误，任务会在后台继续执行
	if isDistributedDDLTimeout(err) {
		logger.Warn("分布式DDL任务超过超时时间，后台继续执行: %v", err)
		return nil
	}
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

	// 打印catalog创建语句
	logger.Info("正在创建StarRocks Catalog: %s", catalogName)
	logger.Debug("=== Catalog创建SQL语句 ===")
	logger.Debug("Catalog名称: %s", catalogName)
	logger.Debug("SQL语句:\n%s", createCatalogSQL)
	logger.Debug("=== Catalog创建SQL语句结束 ===")

	return dm.ExecuteStarRocksSQL(createCatalogSQL)
}

// 移除本地重试逻辑，使用通用的retry包

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

// isDistributedDDLTimeout 判断是否为 ClickHouse 分布式 DDL 超时错误（错误码 159）
func isDistributedDDLTimeout(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "distributed_ddl_task_timeout") {
		return true
	}
	if strings.Contains(msg, "code: 159") {
		return true
	}
	return false
}

// isStarRocksSchemaChangeInProgress 判断StarRocks是否处于表结构变更进行中的错误
// 该错误常见提示为："A schema change operation is in progress on the table ..."
// 以及文档链接提示 SHOW_ALT（不同版本提示文本可能略有差异）
func isStarRocksSchemaChangeInProgress(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "schema change operation is in progress") {
		return true
	}
	if strings.Contains(msg, "show_alt") {
		return true
	}
	if strings.Contains(msg, "error 1064") && strings.Contains(msg, "schema change") {
		return true
	}
	return false
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
	err = retry.QueryRowAndScanWithRetryDefault(db, query, []interface{}{&count}, pair.StarRocks.Database, tableName, columnName)
	if err != nil {
		return false, fmt.Errorf("检查列是否存在失败: %w", err)
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
	err = retry.QueryRowAndScanWithRetryDefault(db, query, []interface{}{&tableType}, pair.StarRocks.Database, tableName)
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
	err = retry.QueryRowAndScanWithRetryDefault(db, query, []interface{}{&tableType}, pair.StarRocks.Database, tableName)
	if err != nil {
		// 如果查询结果为"no rows in result set"，说明表/视图不存在，返回false而不是错误
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("检查表 %s.%s 类型失败: %w",
			pair.StarRocks.Database, tableName, err)
	}

	// 检查是否为VIEW
	return strings.ToUpper(tableType) == "VIEW", nil
}

// CheckStarRocksTableExists 检查StarRocks表是否存在
func (dm *DatabasePairManager) CheckStarRocksTableExists(tableName string) (bool, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return false, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 使用information_schema.tables查询表是否存在
	query := `
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE table_schema = ? 
		AND table_name = ?
	`

	var count int
	err = retry.QueryRowAndScanWithRetryDefault(db, query, []interface{}{&count}, pair.StarRocks.Database, tableName)
	if err != nil {
		return false, fmt.Errorf("检查表 %s.%s 是否存在失败: %w",
			pair.StarRocks.Database, tableName, err)
	}

	return count > 0, nil
}

// CheckStarRocksIndexExists 检查StarRocks表中指定索引是否存在
func (dm *DatabasePairManager) CheckStarRocksIndexExists(tableName, indexName string) (bool, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return false, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 使用information_schema查询索引是否存在
	query := `
		SELECT COUNT(*) 
		FROM information_schema.statistics 
		WHERE table_schema = ? 
		AND table_name = ? 
		AND index_name = ?
	`

	var count int
	err = retry.QueryRowAndScanWithRetryDefault(db, query, []interface{}{&count}, pair.StarRocks.Database, tableName, indexName)
	if err != nil {
		return false, fmt.Errorf("检查索引 %s.%s.%s 是否存在失败: %w",
			pair.StarRocks.Database, tableName, indexName, err)
	}

	return count > 0, nil
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
	err = retry.QueryRowAndScanWithRetryDefault(db, query, []interface{}{&count}, pair.ClickHouse.Database, tableName, columnName)
	if err != nil {
		return false, fmt.Errorf("检查ClickHouse字段 %s.%s.%s 是否存在失败: %w",
			pair.ClickHouse.Database, tableName, columnName, err)
	}

	return count > 0, nil
}

// GetClickHouseViewNames 获取ClickHouse数据库中所有视图名称
func (dm *DatabasePairManager) GetClickHouseViewNames() ([]string, error) {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 查询所有视图
	query := `
		SELECT name 
		FROM system.tables 
		WHERE database = ? AND engine = 'View'
		ORDER BY name
	`

	// 使用重试机制获取视图列表
	rows, err := retry.QueryWithRetryDefault(db, query, pair.ClickHouse.Database)
	if err != nil {
		return nil, fmt.Errorf("查询ClickHouse视图列表失败: %w", err)
	}
	defer rows.Close()

	var viewNames []string
	for rows.Next() {
		var viewName string
		if err := rows.Scan(&viewName); err != nil {
			return nil, fmt.Errorf("扫描视图名称失败: %w", err)
		}
		viewNames = append(viewNames, viewName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历视图列表失败: %w", err)
	}

	logger.Debug("获取到 %d 个ClickHouse视图", len(viewNames))
	return viewNames, nil
}

// GetClickHouseTableColumns 获取ClickHouse表的所有列信息
func (dm *DatabasePairManager) GetClickHouseTableColumns(tableName string) ([]string, error) {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 查询表的所有列
	query := `
		SELECT name 
		FROM system.columns 
		WHERE database = ? AND table = ?
		ORDER BY position
	`

	// 使用重试机制获取表字段列表
	rows, err := retry.QueryWithRetryDefault(db, query, pair.ClickHouse.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询ClickHouse表 %s 列信息失败: %w", tableName, err)
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("扫描列名称失败: %w", err)
		}
		columnNames = append(columnNames, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历列列表失败: %w", err)
	}

	logger.Debug("获取到表 %s 的 %d 个列", tableName, len(columnNames))
	return columnNames, nil
}

// GetStarRocksTableColumns 获取StarRocks表的所有列信息
func (dm *DatabasePairManager) GetStarRocksTableColumns(tableName string) ([]string, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 查询表的所有列
	query := `
		SELECT COLUMN_NAME 
		FROM information_schema.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	// 使用重试机制获取表字段列表
	rows, err := retry.QueryWithRetryDefault(db, query, pair.StarRocks.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询StarRocks表 %s 列信息失败: %w", tableName, err)
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("扫描列名称失败: %w", err)
		}
		columnNames = append(columnNames, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历列列表失败: %w", err)
	}

	logger.Debug("获取到表 %s 的 %d 个列", tableName, len(columnNames))
	return columnNames, nil
}

// GetStarRocksTableIndexes 获取StarRocks表的所有索引名称
func (dm *DatabasePairManager) GetStarRocksTableIndexes(tableName string) ([]string, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 使用information_schema查询表的所有索引
	query := `
		SELECT DISTINCT index_name 
		FROM information_schema.statistics 
		WHERE table_schema = ? 
		AND table_name = ?
		AND index_name != 'PRIMARY'
	`

	// 使用重试机制获取表索引列表
	rows, err := retry.QueryWithRetryDefault(db, query, pair.StarRocks.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询表 %s.%s 的索引失败: %w",
			pair.StarRocks.Database, tableName, err)
	}
	defer rows.Close()

	var indexNames []string
	for rows.Next() {
		var indexName string
		if err := rows.Scan(&indexName); err != nil {
			return nil, fmt.Errorf("读取索引名称失败: %w", err)
		}
		indexNames = append(indexNames, indexName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历索引结果失败: %w", err)
	}

	logger.Debug("获取到表 %s 的 %d 个索引", tableName, len(indexNames))
	return indexNames, nil
}

// ExecuteRollbackSQL 执行回退SQL语句
func (dm *DatabasePairManager) ExecuteRollbackSQL(sqlStatements []string, isClickHouse bool) error {
	logger.Info("开始执行回退SQL，共 %d 条语句", len(sqlStatements))

	for i, sql := range sqlStatements {
		if strings.TrimSpace(sql) == "" {
			continue
		}

		logger.Debug("[%d/%d] 执行回退SQL: %s", i+1, len(sqlStatements), sql)

		var err error
		if isClickHouse {
			err = dm.ExecuteClickHouseSQL(sql)
		} else {
			err = dm.ExecuteStarRocksSQL(sql)
		}

		if err != nil {
			logger.Error("执行回退SQL失败: %s, 错误: %v", sql, err)
			return fmt.Errorf("执行回退SQL失败: %w", err)
		}

		logger.Debug("[%d/%d] 回退SQL执行成功", i+1, len(sqlStatements))
	}

	logger.Info("所有回退SQL执行完成")
	return nil
}
