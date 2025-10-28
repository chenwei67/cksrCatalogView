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
	HTTPPort int    `json:"http_port"` // HTTP端口，用于JDBC连接
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

// DatabasePair 数据库对配置，包含一个ClickHouse和一个StarRocks数据库
type DatabasePair struct {
	Name          string         `json:"name"`            // 数据库对的名称标识
	CatalogName   string         `json:"catalog_name"`    // StarRocks中的Catalog名称
	SRTableSuffix string         `json:"sr_table_suffix"` // StarRocks表统一后缀（用于批量重命名）
	ClickHouse    DatabaseConfig `json:"clickhouse"`
	StarRocks     DatabaseConfig `json:"starrocks"`
}

// LogConfig 日志配置
type LogConfig struct {
	// 是否启用文件日志
	EnableFileLog bool `json:"enable_file_log"`
	// 日志文件路径，如果为空且启用文件日志，则使用临时目录
	LogFilePath string `json:"log_file_path"`
	// 日志级别
	LogLevel string `json:"log_level"`
}

// ViewUpdaterConfig 视图更新器配置
type ViewUpdaterConfig struct {
	// 是否启用视图更新器
	Enabled bool `json:"enabled"`
	// Cron表达式，定义更新时间
	CronExpression string `json:"cron_expression"`
	// 是否启用调试模式（使用虚拟锁）
	DebugMode bool `json:"debug_mode"`
	// K8s命名空间（非调试模式时使用）
	K8sNamespace string `json:"k8s_namespace"`
	// Lease名称
	LeaseName string `json:"lease_name"`
	// 实例标识
	Identity string `json:"identity"`
	// 锁持有时间（秒）
	LockDurationSeconds int `json:"lock_duration_seconds"`
}
// TimestampColumnConfig 时间戳列配置
type TimestampColumnConfig struct {
	// 列名，用于替换默认的recordTimestamp
	Column string `json:"column"`
	// 数据类型，支持的类型：datetime, timestamp, bigint等
	Type string `json:"type"`
}

// RetryConfig 重试配置
type RetryConfig struct {
	// 最大重试次数
	MaxRetries int `json:"max_retries"`
	// 重试间隔（毫秒）
	DelayMs int `json:"delay_ms"`
}

// Config 应用配置
type Config struct {
	// 多数据库对配置
	DatabasePairs []DatabasePair `json:"database_pairs"`

	// 忽略的表列表，这些表不会被处理
	IgnoreTables []string `json:"ignore_tables"`

	// 时间戳列配置，格式为 "表名": {"column": "列名", "type": "数据类型"}
	TimestampColumns map[string]TimestampColumnConfig `json:"timestamp_columns"`

	TempDir   string      `json:"temp_dir"`
	DriverURL string      `json:"driver_url"`
	Log       LogConfig   `json:"log"`
	Retry     RetryConfig `json:"retry"`
	// 视图更新器配置
	ViewUpdater ViewUpdaterConfig `json:"view_updater"`
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

	// 设置日志配置默认值
	if config.Log.LogLevel == "" {
		config.Log.LogLevel = "INFO"
	}

	// 设置视图更新器配置默认值
	if config.ViewUpdater.CronExpression == "" {
		config.ViewUpdater.CronExpression = "0 0 2 * * *" // 每天凌晨2点
	}
	if config.ViewUpdater.K8sNamespace == "" {
		config.ViewUpdater.K8sNamespace = "default"
	}
	if config.ViewUpdater.LeaseName == "" {
		config.ViewUpdater.LeaseName = "cksr-view-updater"
	}
	if config.ViewUpdater.Identity == "" {
		config.ViewUpdater.Identity = "cksr-instance"
	}
	if config.ViewUpdater.LockDurationSeconds == 0 {
		config.ViewUpdater.LockDurationSeconds = 300 // 5分钟
	}
	
	// 设置重试配置默认值
	if config.Retry.MaxRetries == 0 {
		config.Retry.MaxRetries = 3
	}
	if config.Retry.DelayMs == 0 {
		config.Retry.DelayMs = 100
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
	return fmt.Sprintf("jdbc:clickhouse://%s:%d/?database=%s&autoCommit=true&socket_timeout=300000&connection_timeout=30000&compress=true&allow_jdbctemplate_transactions=0",
		pair.ClickHouse.Host, pair.ClickHouse.HTTPPort, pair.ClickHouse.Database)
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
