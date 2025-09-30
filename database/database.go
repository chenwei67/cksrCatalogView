/*
 * @File : database
 * @Date : 2025/1/27
 * @Author : Assistant
 * @Version: 1.0.0
 * @Description: 数据库连接和操作功能
 */

package database

import (
	"database/sql"
	"fmt"
	"strings"

	"cksr/config"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
)

// DatabaseManager 数据库管理器
type DatabaseManager struct {
	config *config.Config
}

// NewDatabaseManager 创建数据库管理器
func NewDatabaseManager(cfg *config.Config) *DatabaseManager {
	return &DatabaseManager{config: cfg}
}

// GetClickHouseConnection 获取ClickHouse连接
func (dm *DatabaseManager) GetClickHouseConnection() (*sql.DB, error) {
	db, err := sql.Open("clickhouse", dm.config.GetClickHouseDSN())
	if err != nil {
		return nil, fmt.Errorf("连接ClickHouse失败: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ClickHouse连接测试失败: %w", err)
	}

	return db, nil
}

// GetStarRocksConnection 获取StarRocks连接
func (dm *DatabaseManager) GetStarRocksConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", dm.config.GetStarRocksDSN())
	if err != nil {
		return nil, fmt.Errorf("连接StarRocks失败: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("StarRocks连接测试失败: %w", err)
	}

	return db, nil
}

// ExportClickHouseTables 导出ClickHouse数据库中所有表的CREATE TABLE语句
func (dm *DatabaseManager) ExportClickHouseTables() (map[string]string, error) {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 获取所有表名
	query := fmt.Sprintf("SHOW TABLES FROM %s", dm.config.ClickHouse.Database)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("获取ClickHouse表列表失败: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("扫描表名失败: %w", err)
		}
		tables = append(tables, tableName)
	}

	// 导出每个表的CREATE TABLE语句
	tableSchemas := make(map[string]string)
	for _, tableName := range tables {
		createQuery := fmt.Sprintf("SHOW CREATE TABLE %s.%s", dm.config.ClickHouse.Database, tableName)
		var createSQL string
		err := db.QueryRow(createQuery).Scan(&createSQL)
		if err != nil {
			return nil, fmt.Errorf("获取表%s的CREATE语句失败: %w", tableName, err)
		}
		tableSchemas[tableName] = createSQL
	}

	return tableSchemas, nil
}

// ExportStarRocksTables 导出StarRocks数据库中所有表的CREATE TABLE语句
func (dm *DatabaseManager) ExportStarRocksTables() (map[string]string, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 获取所有表名
	query := fmt.Sprintf("SHOW TABLES FROM %s", dm.config.StarRocks.Database)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("获取StarRocks表列表失败: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("扫描表名失败: %w", err)
		}
		tables = append(tables, tableName)
	}

	// 导出每个表的CREATE TABLE语句
	tableSchemas := make(map[string]string)
	for _, tableName := range tables {
		createQuery := fmt.Sprintf("SHOW CREATE TABLE %s.%s", dm.config.StarRocks.Database, tableName)
		rows, err := db.Query(createQuery)
		if err != nil {
			return nil, fmt.Errorf("获取表%s的CREATE语句失败: %w", tableName, err)
		}
		defer rows.Close()

		if rows.Next() {
			var table, createSQL string
			if err := rows.Scan(&table, &createSQL); err != nil {
				return nil, fmt.Errorf("扫描CREATE语句失败: %w", err)
			}
			tableSchemas[tableName] = createSQL
		}
	}

	return tableSchemas, nil
}

// ExecuteClickHouseSQL 在ClickHouse中执行SQL语句
func (dm *DatabaseManager) ExecuteClickHouseSQL(sql string) error {
	db, err := dm.GetClickHouseConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(sql)
	if err != nil {
		return fmt.Errorf("执行ClickHouse SQL失败: %w", err)
	}

	return nil
}

// ExecuteStarRocksSQL 在StarRocks中执行SQL语句
func (dm *DatabaseManager) ExecuteStarRocksSQL(sql string) error {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(sql)
	if err != nil {
		return fmt.Errorf("执行StarRocks SQL失败: %w", err)
	}

	return nil
}

// CreateStarRocksCatalog 在StarRocks中创建ClickHouse Catalog
func (dm *DatabaseManager) CreateStarRocksCatalog(catalogName string) error {
	catalogSQL := fmt.Sprintf(`
CREATE EXTERNAL CATALOG IF NOT EXISTS %s
PROPERTIES (
    "type" = "jdbc",
    "user" = "%s",
    "password" = "%s",
    "jdbc_uri" = "%s",
    "driver_url" = "%s",
    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
);`, catalogName,
		dm.config.ClickHouse.Username,
		dm.config.ClickHouse.Password,
		dm.config.GetClickHouseJDBCURI(),
		dm.config.DriverURL)

	return dm.ExecuteStarRocksSQL(catalogSQL)
}

// ExecuteBatchSQL 批量执行SQL语句
func (dm *DatabaseManager) ExecuteBatchSQL(sqls []string, isClickHouse bool) error {
	for _, sql := range sqls {
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