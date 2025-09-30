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

// Config 应用配置
type Config struct {
	ClickHouse DatabaseConfig `json:"clickhouse"`
	StarRocks  DatabaseConfig `json:"starrocks"`
	TempDir    string         `json:"temp_dir"`
	DriverURL  string         `json:"driver_url"`
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

	return &config, nil
}

// GetClickHouseDSN 获取ClickHouse连接字符串
func (c *Config) GetClickHouseDSN() string {
	return fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
		c.ClickHouse.Host, c.ClickHouse.Port, c.ClickHouse.Database,
		c.ClickHouse.Username, c.ClickHouse.Password)
}

// GetStarRocksDSN 获取StarRocks连接字符串
func (c *Config) GetStarRocksDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		c.StarRocks.Username, c.StarRocks.Password,
		c.StarRocks.Host, c.StarRocks.Port, c.StarRocks.Database)
}

// GetClickHouseJDBCURI 获取ClickHouse JDBC连接字符串
func (c *Config) GetClickHouseJDBCURI() string {
	return fmt.Sprintf("jdbc:clickhouse://%s:%d/?database=%s&autoCommit=true&socket_timeout=300000&connection_timeout=10000&compress=true",
		c.ClickHouse.Host, c.ClickHouse.Port, c.ClickHouse.Database)
}