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
    "time"

    "cksr/builder"
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

// ExecuteBatchSQLWithRetry 批量执行SQL语句，支持重试机制
func (dm *DatabasePairManager) ExecuteBatchSQLWithRetry(sqlStatements []string, isClickHouse bool, maxRetries int, retryDelay time.Duration) error {
    for i, sql := range sqlStatements {
        sql = strings.TrimSpace(sql)
        if sql == "" {
            continue
        }

		// 对每个SQL语句进行重试
		var lastErr error
		for attempt := 1; attempt <= maxRetries; attempt++ {
			logger.Debug("执行SQL语句 [%d/%d]，第 %d 次尝试: %s", i+1, len(sqlStatements), attempt, sql)
			
			var err error
            if isClickHouse {
                err = dm.ExecuteClickHouseSQL(sql)
            } else {
                err = dm.ExecuteStarRocksSQL(sql)
            }
            if err == nil {
                logger.Debug("SQL语句 [%d/%d] 执行成功", i+1, len(sqlStatements))
                break
            }

			lastErr = err
			if attempt < maxRetries {
				logger.Warn("SQL语句 [%d/%d] 第 %d 次执行失败，%v 秒后重试: %v", i+1, len(sqlStatements), attempt, retryDelay.Seconds(), err)
				time.Sleep(retryDelay)
			} else {
				logger.Error("SQL语句 [%d/%d] 经过 %d 次重试后仍然失败: %v", i+1, len(sqlStatements), maxRetries, err)
			}
        }

        if lastErr != nil {
            return fmt.Errorf("执行SQL失败 (经过 %d 次重试): %s, 错误: %w", maxRetries, sql, lastErr)
        }
    }
    return nil
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
	err = db.QueryRow(query, pair.StarRocks.Database, tableName, indexName).Scan(&count)
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
	
	// 使用SRAddColumnBuilder构建SQL语句
	builder := builder.NewSRAddColumnBuilder(pair.StarRocks.Database, tableName)
	builder.AddSyncFromCKColumn()
	
	// 分别获取字段和索引SQL
	columnSQL := builder.Build()
	indexSQLs := builder.BuildIndexes()
	
	logger.Debug("正在为表 %s 添加 syncFromCK 字段和索引...", tableName)
	
	// 第一步：添加字段
	logger.Debug("执行字段添加SQL: %s", columnSQL)
	// 对于 SR 正在进行 schema change 的场景，持续重试，直到成功或字段已存在
	retryDelayForSchema := time.Second * 5
	for {
		// 避免重复添加：每次尝试前都检查字段是否已存在
		existsNow, checkErr := dm.CheckStarRocksColumnExists(tableName, "syncFromCK")
		if checkErr == nil && existsNow {
			logger.Debug("检测到字段 syncFromCK 已存在，跳过添加步骤")
			break
		}

		if err := dm.ExecuteStarRocksSQL(columnSQL); err != nil {
			if isStarRocksSchemaChangeInProgress(err) {
				logger.Warn("表 %s 正在进行schema变更，%d秒后重试添加字段: %v", tableName, int(retryDelayForSchema.Seconds()), err)
				time.Sleep(retryDelayForSchema)
				continue
			}
			return fmt.Errorf("为表 %s 添加字段失败: %w", tableName, err)
		}
		// 执行成功，跳出循环
		break
	}
	
	// 等待DDL操作完成，给StarRocks时间更新元数据
	logger.Debug("等待StarRocks元数据更新...")
	time.Sleep(time.Second * 3)
	
	// 第二步：验证字段是否成功添加（带重试机制）
	logger.Debug("验证字段是否成功添加...")
	var columnExists bool
	maxRetries := 1600
	retryDelay := time.Second * 2
	
	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Debug("第 %d 次验证字段存在性...", attempt)
		
		exists, err := dm.CheckStarRocksColumnExists(tableName, "syncFromCK")
		if err != nil {
			if attempt == maxRetries {
				return fmt.Errorf("验证字段添加结果失败 (尝试 %d/%d): %w", attempt, maxRetries, err)
			}
			logger.Warn("第 %d 次验证失败，%v 秒后重试: %v", attempt, retryDelay.Seconds(), err)
			time.Sleep(retryDelay)
			continue
		}
		
		if exists {
			columnExists = true
			logger.Debug("第 %d 次验证成功，字段已存在", attempt)
			break
		}
		
		if attempt < maxRetries {
			logger.Debug("第 %d 次验证字段不存在，%v 秒后重试...", attempt, retryDelay.Seconds())
			time.Sleep(retryDelay)
		}
	}
	
	if !columnExists {
		return fmt.Errorf("字段添加后验证失败，经过 %d 次重试后 syncFromCK 字段仍不存在于表 %s", maxRetries, tableName)
	}
	
	logger.Debug("字段 syncFromCK 添加成功，开始创建索引...")
	
	// 第三步：创建索引
	for i, indexSQL := range indexSQLs {
		logger.Debug("执行索引创建SQL [%d/%d]: %s", i+1, len(indexSQLs), indexSQL)
		
		// 检查索引是否已存在
		indexName := "idx_syncFromCK" // 默认索引名
		if strings.Contains(indexSQL, "`idx_syncFromCK`") {
			indexExists, checkErr := dm.CheckStarRocksIndexExists(tableName, indexName)
			if checkErr != nil {
				logger.Warn("检查索引 %s 是否存在失败: %v，继续尝试创建", indexName, checkErr)
			} else if indexExists {
				logger.Debug("索引 %s 已存在，跳过创建", indexName)
				continue
			}
		}
		
		// 索引创建也可能受到正在进行的schema变更影响，这里对该错误做容忍并重试
		for {
			err := dm.ExecuteStarRocksSQL(indexSQL)
			if err == nil {
				break
			}
			// 如果是索引已存在的错误，记录警告但不中断流程
			if strings.Contains(err.Error(), "Duplicate key name") || 
			   strings.Contains(err.Error(), "already exists") {
				logger.Warn("索引可能已存在，跳过: %v", err)
				break
			}

			if isStarRocksSchemaChangeInProgress(err) {
				logger.Warn("表 %s 存在schema变更进行中，延迟%ds后重试创建索引: %v", tableName, int(retryDelayForSchema.Seconds()), err)
				time.Sleep(retryDelayForSchema)
				continue
			}
			return fmt.Errorf("为表 %s 创建索引失败 [%d/%d]: %w", tableName, i+1, len(indexSQLs), err)
		}
	}

	logger.Debug("表 %s 的 syncFromCK 字段和索引添加成功", tableName)
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
	
	rows, err := db.Query(query, pair.ClickHouse.Database)
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
	
	rows, err := db.Query(query, pair.ClickHouse.Database, tableName)
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
	
	rows, err := db.Query(query, pair.StarRocks.Database, tableName)
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

	rows, err := db.Query(query, pair.StarRocks.Database, tableName)
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
