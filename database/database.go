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
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return "", err
	}
	defer db.Close()

	pair := dm.config.DatabasePairs[dm.pairIndex]

	createQuery := fmt.Sprintf("SHOW CREATE TABLE %s.%s", pair.StarRocks.Database, tableName)
	rows, err := db.Query(createQuery)
	if err != nil {
		return "", fmt.Errorf("获取表 %s 的创建语句失败: %w", tableName, err)
	}
	defer rows.Close()

	if rows.Next() {
		var table, createStatement string
		if err := rows.Scan(&table, &createStatement); err != nil {
			return "", fmt.Errorf("扫描表 %s 的创建语句失败: %w", tableName, err)
		}
		return createStatement, nil
	}

	return "", fmt.Errorf("未找到表 %s 的创建语句", tableName)
}

// ExportStarRocksTables 导出StarRocks表结构
func (dm *DatabasePairManager) ExportStarRocksTables() (map[string]string, error) {
	tableNames, err := dm.GetStarRocksTableNames()
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, tableName := range tableNames {
		ddl, err := dm.GetStarRocksTableDDL(tableName)
		if err != nil {
			log.Printf("获取表 %s 的DDL失败: %v", tableName, err)
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
