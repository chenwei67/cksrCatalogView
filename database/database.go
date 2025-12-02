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
	"sync"
	"time"

	"cksr/config"
	"cksr/logger"

	"example.com/migrationLib/database"
	"example.com/migrationLib/retry"

	"cksr/parser"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
)

// ClickHouse 设置项常量，避免使用魔字符串
const (
	// 分布式 DDL 任务超时设置键名
	ClickHouseSettingDistributedDDLTaskTimeout = "distributed_ddl_task_timeout"
	// StarRocks 表类型常量
	StarRocksTableTypeBaseTable = "BASE TABLE"
	StarRocksTableTypeView      = "VIEW"
)

// DatabasePairManager 数据库对管理器
type DatabasePairManager struct {
	config    *config.Config
	pairIndex int
	// Pooled connections for concurrent reuse
	clickhouseDB *sql.DB
	starrocksDB  *sql.DB
	retryConfig  retry.Config
}

// Global registry to close all managers on process exit
var managerRegistry struct {
	mu   sync.Mutex
	list []*DatabasePairManager
}

func registerManager(dm *DatabasePairManager) {
	managerRegistry.mu.Lock()
	managerRegistry.list = append(managerRegistry.list, dm)
	managerRegistry.mu.Unlock()
}

// CloseAll closes all registered managers (used by CLI commands on exit)
func CloseAll() {
	managerRegistry.mu.Lock()
	defer managerRegistry.mu.Unlock()
	for _, dm := range managerRegistry.list {
		if dm != nil {
			dm.Close()
		}
	}
	managerRegistry.list = nil
}

// NewDatabasePairManager 创建数据库对管理器（通过索引）
func NewDatabasePairManager(cfg *config.Config, pairIndex int) *DatabasePairManager {
	dm := &DatabasePairManager{
		config:    cfg,
		pairIndex: pairIndex,
		retryConfig: retry.Config{
			MaxRetries: cfg.Retry.MaxRetries,
			Delay:      time.Duration(cfg.Retry.DelayMs) * time.Millisecond,
		},
	}
	registerManager(dm)
	return dm
}

// NewDatabasePairManagerByName 创建数据库对管理器（通过名称）
func NewDatabasePairManagerByName(cfg *config.Config, pairName string) *DatabasePairManager {
	for i, pair := range cfg.DatabasePairs {
		if pair.Name == pairName {
			dm := &DatabasePairManager{
				config:    cfg,
				pairIndex: i,
				retryConfig: retry.Config{
					MaxRetries: cfg.Retry.MaxRetries,
					Delay:      time.Duration(cfg.Retry.DelayMs) * time.Millisecond,
				},
			}
			registerManager(dm)
			return dm
		}
	}
	return nil
}

// GetRetryConfig 获取重试配置
func (dm *DatabasePairManager) GetRetryConfig() retry.Config {
	return dm.retryConfig
}

// GetClickHouseConnection 获取ClickHouse连接
func (dm *DatabasePairManager) GetClickHouseConnection() (*sql.DB, error) {
	if dm.clickhouseDB == nil {
		return nil, fmt.Errorf("ClickHouse连接未初始化，请先调用 Init()")
	}
	return dm.clickhouseDB, nil
}

// GetStarRocksConnection 获取StarRocks连接
func (dm *DatabasePairManager) GetStarRocksConnection() (*sql.DB, error) {
	if dm.starrocksDB == nil {
		return nil, fmt.Errorf("StarRocks连接未初始化，请先调用 Init()")
	}
	return dm.starrocksDB, nil
}

// Close closes pooled connections gracefully
func (dm *DatabasePairManager) Close() {
	if dm.clickhouseDB != nil {
		_ = dm.clickhouseDB.Close()
		dm.clickhouseDB = nil
	}
	if dm.starrocksDB != nil {
		_ = dm.starrocksDB.Close()
		dm.starrocksDB = nil
	}
}

// Init proactively initializes both CK and SR connections and applies pool config
func (dm *DatabasePairManager) Init() error {
	// ClickHouse
	if dm.clickhouseDB == nil {
		dsn := dm.config.GetClickHouseDSNByIndex(dm.pairIndex)
		if dsn == "" {
			return fmt.Errorf("无效的数据库对索引: %d", dm.pairIndex)
		}
		db, err := sql.Open("clickhouse", dsn)
		if err != nil {
			return fmt.Errorf("连接ClickHouse失败: %w", err)
		}
		pair := dm.config.DatabasePairs[dm.pairIndex]
		if pair.ClickHouse.PoolMaxOpenConns > 0 {
			db.SetMaxOpenConns(pair.ClickHouse.PoolMaxOpenConns)
		}
		if pair.ClickHouse.PoolMaxIdleConns > 0 {
			db.SetMaxIdleConns(pair.ClickHouse.PoolMaxIdleConns)
		}
		if pair.ClickHouse.PoolConnMaxIdleSeconds > 0 {
			db.SetConnMaxIdleTime(time.Duration(pair.ClickHouse.PoolConnMaxIdleSeconds) * time.Second)
		}
		if pair.ClickHouse.PoolConnMaxLifetimeSeconds > 0 {
			db.SetConnMaxLifetime(time.Duration(pair.ClickHouse.PoolConnMaxLifetimeSeconds) * time.Second)
		}
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return fmt.Errorf("ClickHouse连接测试失败: %w", err)
		}
		dm.clickhouseDB = db
	}
	// StarRocks
	if dm.starrocksDB == nil {
		dsn := dm.config.GetStarRocksDSNByIndex(dm.pairIndex)
		if dsn == "" {
			return fmt.Errorf("无效的数据库对索引: %d", dm.pairIndex)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return fmt.Errorf("连接StarRocks失败: %w", err)
		}
		pair := dm.config.DatabasePairs[dm.pairIndex]
		if pair.StarRocks.PoolMaxOpenConns > 0 {
			db.SetMaxOpenConns(pair.StarRocks.PoolMaxOpenConns)
		}
		if pair.StarRocks.PoolMaxIdleConns > 0 {
			db.SetMaxIdleConns(pair.StarRocks.PoolMaxIdleConns)
		}
		if pair.StarRocks.PoolConnMaxIdleSeconds > 0 {
			db.SetConnMaxIdleTime(time.Duration(pair.StarRocks.PoolConnMaxIdleSeconds) * time.Second)
		}
		if pair.StarRocks.PoolConnMaxLifetimeSeconds > 0 {
			db.SetConnMaxLifetime(time.Duration(pair.StarRocks.PoolConnMaxLifetimeSeconds) * time.Second)
		}
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return fmt.Errorf("StarRocks连接测试失败: %w", err)
		}
		dm.starrocksDB = db
	}
	return nil
}

// ExportClickHouseTables 导出ClickHouse表结构
func (dm *DatabasePairManager) ExportClickHouseTables() (map[string]string, error) {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return nil, err
	}

	// 使用重试机制获取表列表
	rows, err := retry.QueryWithRetry(db, dm.retryConfig, "SHOW TABLES")
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
		if err := retry.QueryRowAndScanWithRetry(db, dm.retryConfig, createQuery, []interface{}{&createStatement}); err != nil {
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

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 使用重试机制获取表列表
	rows, err := retry.QueryWithRetry(db, dm.retryConfig, fmt.Sprintf("SHOW TABLES FROM %s", pair.StarRocks.Database))
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

	pair := dm.config.DatabasePairs[dm.pairIndex]
	query := `
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = ?
    `

	rows, err := retry.QueryWithRetry(db, dm.retryConfig, query, pair.StarRocks.Database)
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

	pair := dm.config.DatabasePairs[dm.pairIndex]
	createQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", pair.StarRocks.Database, tableName)

	err = retry.QueryRowAndScanWithRetry(db, dm.retryConfig, createQuery, []interface{}{&name, &ddl})
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

	// 增加分布式 DDL 超时时间，避免 ON CLUSTER 任务因超时而提前返回错误（从CK配置读取）
	pair := dm.config.DatabasePairs[dm.pairIndex]
	timeout := pair.ClickHouse.DistributedDDLTaskTimeoutSeconds
	if timeout <= 0 {
		timeout = 3600
	}
	if err = retry.ExecWithRetry(db, dm.retryConfig, fmt.Sprintf("SET %s = %d", ClickHouseSettingDistributedDDLTaskTimeout, timeout)); err != nil {
		return fmt.Errorf("设置 ClickHouse 分布式DDL超时失败: %w", err)
	}
	// 执行原始 SQL（不注入 query_id，兼容旧版本）
	err = retry.ExecWithRetry(db, dm.retryConfig, sql)
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

	return retry.ExecWithRetry(db, dm.retryConfig, sql)
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

// ExportClickHouseTablesAsParserTables 导出ClickHouse表结构
func (dm *DatabasePairManager) ExportClickHouseTablesAsParserTables() (map[string]parser.Table, error) {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return nil, err
	}

	pair := dm.config.DatabasePairs[dm.pairIndex]

	// 调用 migrationLab 的实现
	// 注意：需要适配返回类型，因为 parser 包路径不同
	labTables, err := database.ExportClickHouseTablesAsParserTables(db, dm.retryConfig, pair.ClickHouse.Database)
	if err != nil {
		return nil, err
	}

	// 转换 parser.Table 类型 (从 migrationLab/parser 到 cksr/parser)
	// 由于使用了类型别名 (type Table = parser.Table)，它们是相同的类型，可以直接赋值
	res := make(map[string]parser.Table, len(labTables))
	for name, labTable := range labTables {
		res[name] = labTable
	}
	return res, nil
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

// ExecuteRollbackSQLWithDB 执行回退SQL（传入db）
func (dm *DatabasePairManager) ExecuteRollbackSQLWithDB(db *sql.DB, sqlStatements []string, isClickHouse bool) error {
	logger.Info("开始执行回退SQL，共 %d 条语句", len(sqlStatements))
	for i, s := range sqlStatements {
		if strings.TrimSpace(s) == "" {
			continue
		}
		logger.Debug("[%d/%d] 执行回退SQL: %s", i+1, len(sqlStatements), s)
		var err error
		if isClickHouse {
			err = dm.ExecuteClickHouseSQLWithDB(db, s)
		} else {
			err = dm.ExecuteStarRocksSQLWithDB(db, s)
		}
		if err != nil {
			logger.Error("执行回退SQL失败: %s, 错误: %v", s, err)
			return fmt.Errorf("执行回退SQL失败: %w", err)
		}
		logger.Debug("[%d/%d] 回退SQL执行成功", i+1, len(sqlStatements))
	}
	logger.Info("所有回退SQL执行完成")
	return nil
}

// ExecuteClickHouseSQLWithDB 执行ClickHouse SQL（传入已初始化的db）
func (dm *DatabasePairManager) ExecuteClickHouseSQLWithDB(db *sql.DB, sql string) error {
	// 增加分布式 DDL 超时时间设置
	pair := dm.config.DatabasePairs[dm.pairIndex]
	timeout := pair.ClickHouse.DistributedDDLTaskTimeoutSeconds
	if timeout <= 0 {
		timeout = 3600
	}
	if err := retry.ExecWithRetry(db, dm.retryConfig, fmt.Sprintf("SET %s = %d", ClickHouseSettingDistributedDDLTaskTimeout, timeout)); err != nil {
		return fmt.Errorf("设置 ClickHouse 分布式DDL超时失败: %w", err)
	}
	err := retry.ExecWithRetry(db, dm.retryConfig, sql)
	if isDistributedDDLTimeout(err) {
		return fmt.Errorf("ClickHouse 分布式DDL超时(159): %w", err)
	}
	return err
}

// ExecuteStarRocksSQLWithDB 执行StarRocks SQL（传入已初始化的db）
func (dm *DatabasePairManager) ExecuteStarRocksSQLWithDB(db *sql.DB, sql string) error {
	return retry.ExecWithRetry(db, dm.retryConfig, sql)
}

// ExecuteBatchSQLWithDB 批量执行SQL语句（传入db）
func (dm *DatabasePairManager) ExecuteBatchSQLWithDB(db *sql.DB, sqlStatements []string, isClickHouse bool) error {
	for _, s := range sqlStatements {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		var err error
		if isClickHouse {
			err = dm.ExecuteClickHouseSQLWithDB(db, s)
		} else {
			err = dm.ExecuteStarRocksSQLWithDB(db, s)
		}
		if err != nil {
			return fmt.Errorf("执行SQL失败: %s, 错误: %w", s, err)
		}
	}
	return nil
}
