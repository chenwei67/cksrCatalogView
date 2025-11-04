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

// ClickHouse 设置项常量，避免使用魔字符串
const (
	// 分布式 DDL 任务超时设置键名
	ClickHouseSettingDistributedDDLTaskTimeout = "distributed_ddl_task_timeout"
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
			return nil, fmt.Errorf("扫描表名失败: %w", err)
		}

		// 使用重试机制获取表的创建语句
		createQuery := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
		var createStatement string
		if err := retry.QueryRowAndScanWithRetryDefault(db, dm.config, createQuery, []interface{}{&createStatement}); err != nil {
			return nil, fmt.Errorf("获取表 %s 的创建语句失败: %w", tableName, err)
		}

		result[tableName] = createStatement
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历表列表失败: %w", err)
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
	rows, err := retry.QueryWithRetryDefault(db, dm.config, fmt.Sprintf("SHOW TABLES FROM %s", pair.StarRocks.Database))
	if err != nil {
		return nil, fmt.Errorf("获取表列表失败: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("扫描表名失败: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历表列表失败: %w", err)
	}

	return tableNames, nil
}

// GetStarRocksTablesTypes 获取StarRocks库中所有表的类型映射（table_name -> table_type）
func (dm *DatabasePairManager) GetStarRocksTablesTypes() (map[string]string, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]
	query := `
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = ?
    `

	rows, err := retry.QueryWithRetryDefault(db, dm.config, query, pair.StarRocks.Database)
	if err != nil {
		return nil, fmt.Errorf("查询StarRocks表类型失败: %w", err)
	}
	defer rows.Close()

	types := make(map[string]string)
	for rows.Next() {
		var name, t string
		if err := rows.Scan(&name, &t); err != nil {
			return nil, fmt.Errorf("扫描StarRocks表类型失败: %w", err)
		}
		types[name] = t
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历StarRocks表类型失败: %w", err)
	}
	return types, nil
}

// GetStarRocksTableDDL 获取StarRocks表的DDL（使用通用重试wrapper）
func (dm *DatabasePairManager) GetStarRocksTableDDL(tableName string) (string, error) {
	var ddl string
	var name string

	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return "", fmt.Errorf("获取StarRocks连接失败: %w", err)
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]
	createQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", pair.StarRocks.Database, tableName)

	err = retry.QueryRowAndScanWithRetryDefault(db, dm.config, createQuery, []interface{}{&name, &ddl})
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

	// 增加分布式 DDL 超时时间，避免 ON CLUSTER 任务因超时而提前返回错误（从CK配置读取）
	pair := dm.config.DatabasePairs[dm.pairIndex]
	timeout := pair.ClickHouse.DistributedDDLTaskTimeoutSeconds
	if timeout <= 0 {
		timeout = 3600
	}
	if err = retry.ExecWithRetryDefault(db, dm.config, fmt.Sprintf("SET %s = %d", ClickHouseSettingDistributedDDLTaskTimeout, timeout)); err != nil {
		return fmt.Errorf("设置 ClickHouse 分布式DDL超时失败: %w", err)
	}
	// 执行原始 SQL（不注入 query_id，兼容旧版本）
	err = retry.ExecWithRetryDefault(db, dm.config, sql)
	// 如果是分布式DDL超时（错误码 159），按错误处理并返回
	if isDistributedDDLTimeout(err) {
		return fmt.Errorf("ClickHouse 分布式DDL超时(159): %w", err)
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

	return retry.ExecWithRetryDefault(db, dm.config, sql)
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
	if strings.Contains(msg, ClickHouseSettingDistributedDDLTaskTimeout) {
		return true
	}
	if strings.Contains(msg, "code: 159") {
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
	err = retry.QueryRowAndScanWithRetryDefault(db, dm.config, query, []interface{}{&tableType}, pair.StarRocks.Database, tableName)
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
	err = retry.QueryRowAndScanWithRetryDefault(db, dm.config, query, []interface{}{&tableType}, pair.StarRocks.Database, tableName)
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

// GetStarRocksTableType 获取StarRocks表类型（如 BASE TABLE / VIEW）。
// 若表不存在，返回底层查询错误（可能为 sql.ErrNoRows），不做吞并，由上层自行判断。
func (dm *DatabasePairManager) GetStarRocksTableType(tableName string) (string, error) {
    db, err := dm.GetStarRocksConnection()
    if err != nil {
        return "", err
    }
    defer db.Close()

    pair := dm.config.DatabasePairs[dm.pairIndex]

    query := `
        SELECT table_type 
        FROM information_schema.tables 
        WHERE table_schema = ? 
        AND table_name = ?
    `

    var tableType string
    err = retry.QueryRowAndScanWithRetryDefault(db, dm.config, query, []interface{}{&tableType}, pair.StarRocks.Database, tableName)
    if err != nil {
        return "", fmt.Errorf("查询表 %s.%s 类型失败: %w", pair.StarRocks.Database, tableName, err)
    }
    return tableType, nil
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
	err = retry.QueryRowAndScanWithRetryDefault(db, dm.config, query, []interface{}{&count}, pair.StarRocks.Database, tableName)
	if err != nil {
		return false, fmt.Errorf("检查表 %s.%s 是否存在失败: %w",
			pair.StarRocks.Database, tableName, err)
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
	err = retry.QueryRowAndScanWithRetryDefault(db, dm.config, query, []interface{}{&count}, pair.ClickHouse.Database, tableName, columnName)
	if err != nil {
		return false, fmt.Errorf("检查ClickHouse字段 %s.%s.%s 是否存在失败: %w",
			pair.ClickHouse.Database, tableName, columnName, err)
	}

	return count > 0, nil
}


// ListStarRocksCatalogs 列出 StarRocks 中的所有 Catalog 名称
func (dm *DatabasePairManager) ListStarRocksCatalogs() ([]string, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := retry.QueryWithRetryDefault(db, dm.config, "SHOW CATALOGS")
	if err != nil {
		return nil, fmt.Errorf("获取Catalog列表失败: %w", err)
	}
	defer rows.Close()

	var catalogs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("扫描Catalog名称失败: %w", err)
		}
		catalogs = append(catalogs, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历Catalog列表失败: %w", err)
	}
	return catalogs, nil
}

// CheckStarRocksCatalogExists 检查指定 Catalog 是否存在
func (dm *DatabasePairManager) CheckStarRocksCatalogExists(catalogName string) (bool, error) {
	catalogs, err := dm.ListStarRocksCatalogs()
	if err != nil {
		return false, err
	}
	for _, c := range catalogs {
		if c == catalogName {
			return true, nil
		}
	}
	return false, nil
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
	rows, err := retry.QueryWithRetryDefault(db, dm.config, query, pair.ClickHouse.Database, tableName)
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

// GetClickHouseTablesColumns 获取ClickHouse库中所有表的列映射（table -> []column）
func (dm *DatabasePairManager) GetClickHouseTablesColumns() (map[string][]string, error) {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]
	query := `
        SELECT table, name
        FROM system.columns
        WHERE database = ?
        ORDER BY table, position
    `

	rows, err := retry.QueryWithRetryDefault(db, dm.config, query, pair.ClickHouse.Database)
	if err != nil {
		return nil, fmt.Errorf("查询ClickHouse所有表列失败: %w", err)
	}
	defer rows.Close()

	cols := make(map[string][]string)
	for rows.Next() {
		var table, name string
		if err := rows.Scan(&table, &name); err != nil {
			return nil, fmt.Errorf("扫描ClickHouse列失败: %w", err)
		}
		cols[table] = append(cols[table], name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历ClickHouse所有表列失败: %w", err)
	}
	return cols, nil
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
