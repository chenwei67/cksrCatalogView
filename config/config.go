/*
 * @File : config
 * @Date : 2025/1/27
 * @Author : Assistant
 * @Version: 1.0.0
 * @Description: 数据库配置结构定义
 */

package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// DatabaseConfig 数据库连接配置
type DatabaseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

// DatabasePair 数据库对配置，包含一个ClickHouse和一个StarRocks数据库
type DatabasePair struct {
	Name        string         `json:"name"`         // 数据库对的名称标识
	CatalogName string         `json:"catalog_name"` // StarRocks中的Catalog名称
	ClickHouse  DatabaseConfig `json:"clickhouse"`
	StarRocks   DatabaseConfig `json:"starrocks"`
}

// Config 应用配置
type Config struct {
	// 多数据库对配置
	DatabasePairs []DatabasePair `json:"database_pairs"`
	
	TempDir   string `json:"temp_dir"`
	DriverURL string `json:"driver_url"`
}

// LoadConfig 从配置文件加载配置
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 设置默认值
	if config.TempDir == "" {
		config.TempDir = "./temp"
	}

	// 验证配置
	if len(config.DatabasePairs) == 0 {
		return nil, fmt.Errorf("至少需要配置一个数据库对")
	}

	return &config, nil
}

// GetClickHouseDSNByIndex 根据索引获取ClickHouse连接字符串
func (c *Config) GetClickHouseDSNByIndex(index int) string {
	if index >= len(c.DatabasePairs) {
		return ""
	}
	pair := c.DatabasePairs[index]
	return fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
		pair.ClickHouse.Host, pair.ClickHouse.Port, pair.ClickHouse.Database,
		pair.ClickHouse.Username, pair.ClickHouse.Password)
}

// GetStarRocksDSNByIndex 根据索引获取StarRocks连接字符串
func (c *Config) GetStarRocksDSNByIndex(index int) string {
	if index >= len(c.DatabasePairs) {
		return ""
	}
	pair := c.DatabasePairs[index]
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		pair.StarRocks.Username, pair.StarRocks.Password,
		pair.StarRocks.Host, pair.StarRocks.Port, pair.StarRocks.Database)
}

// GetClickHouseJDBCURIByIndex 根据索引获取ClickHouse JDBC连接字符串
func (c *Config) GetClickHouseJDBCURIByIndex(index int) string {
	if index >= len(c.DatabasePairs) {
		return ""
	}
	pair := c.DatabasePairs[index]
	return fmt.Sprintf("jdbc:clickhouse://%s:%d/?database=%s&autoCommit=true&socket_timeout=300000&connection_timeout=10000&compress=true",
		pair.ClickHouse.Host, pair.ClickHouse.Port, pair.ClickHouse.Database)
}

// GetDatabasePairByName 根据名称获取数据库对
func (c *Config) GetDatabasePairByName(name string) (*DatabasePair, bool) {
	for _, pair := range c.DatabasePairs {
		if pair.Name == name {
			return &pair, true
		}
	}
	return nil, false
}