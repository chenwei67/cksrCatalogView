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

// ClickHouseConfig ClickHouse 数据库连接配置
type ClickHouseConfig struct {
    Host     string `json:"host"`
    Port     int    `json:"port"`
    HTTPPort int    `json:"http_port"` // HTTP端口，用于JDBC连接
    Username string `json:"username"`
    Password string `json:"password"`
    Database string `json:"database"`
    // 分布式 DDL 超时（秒），用于设置 distributed_ddl_task_timeout
    DistributedDDLTaskTimeoutSeconds int `json:"distributed_ddl_task_timeout_seconds"`
    // 连接超时（秒）- ClickHouse 驱动的总连接/拨号超时
    ConnTimeoutSeconds int `json:"conn_timeout_seconds"`
    // 读超时（秒）- ClickHouse 驱动的读取超时
    ReadTimeoutSeconds int `json:"read_timeout_seconds"`
    // 写超时（秒）- ClickHouse 驱动的写入超时
    WriteTimeoutSeconds int `json:"write_timeout_seconds"`
    // JDBC socket 超时（毫秒）- 生成 JDBC URI 时使用
    JDBCSocketTimeoutMs int `json:"jdbc_socket_timeout_ms"`
    // JDBC 连接超时（毫秒）- 生成 JDBC URI 时使用
    JDBCConnectionTimeoutMs int `json:"jdbc_connection_timeout_ms"`
}

// StarRocksConfig StarRocks 数据库连接配置
type StarRocksConfig struct {
    Host     string `json:"host"`
    Port     int    `json:"port"`
    Username string `json:"username"`
    Password string `json:"password"`
    Database string `json:"database"`
    // 连接超时（秒）- MySQL 驱动拨号超时
    ConnTimeoutSeconds int `json:"conn_timeout_seconds"`
    // 读超时（秒）
    ReadTimeoutSeconds int `json:"read_timeout_seconds"`
    // 写超时（秒）
    WriteTimeoutSeconds int `json:"write_timeout_seconds"`
}

// DatabasePair 数据库对配置，包含一个ClickHouse和一个StarRocks数据库
type DatabasePair struct {
	Name          string           `json:"name"`            // 数据库对的名称标识
	CatalogName   string           `json:"catalog_name"`    // StarRocks中的Catalog名称
	SRTableSuffix string           `json:"sr_table_suffix"` // StarRocks表统一后缀（用于批量重命名）
	ClickHouse    ClickHouseConfig `json:"clickhouse"`
	StarRocks     StarRocksConfig  `json:"starrocks"`
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
	// Cron表达式，定义更新时间
	CronExpression string `json:"cron_expression"`
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
    // 公共分布式锁配置
    Lock LockConfig `json:"lock"`
    // 回滚策略配置
    Rollback RollbackConfig `json:"rollback"`
    // 解析器相关配置
    Parser ParserConfig `json:"parser"`
}

// LockConfig 公共分布式锁配置
type LockConfig struct {
	// 是否启用调试模式（使用虚拟锁）
	DebugMode bool `json:"debug_mode"`
	// K8s命名空间（非调试模式时使用）
	K8sNamespace string `json:"k8s_namespace"`
	// Lease名称
	LeaseName string `json:"lease_name"`
	// 基础实例标识（业务可在此基础上追加后缀）
	Identity string `json:"identity"`
	// 锁持有时间（秒）
	LockDurationSeconds int `json:"lock_duration_seconds"`
}

// RollbackConfig 回滚策略配置
type RollbackConfig struct {
    // 策略："stop_on_error" 或 "continue_on_error"
    Strategy string `json:"strategy"`
}

// ParserConfig 解析器配置
type ParserConfig struct {
    // DDL 解析超时（秒）
    DDLParseTimeoutSeconds int `json:"ddl_parse_timeout_seconds"`
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

    // 设置视图更新器配置默认值
    if config.ViewUpdater.CronExpression == "" {
        config.ViewUpdater.CronExpression = "0 0 2 * * *" // 每天凌晨2点
    }
	// 设置公共锁配置默认值（不回退到 view_updater，只以 lock 为准）
	if config.Lock.K8sNamespace == "" {
		config.Lock.K8sNamespace = "olap"
	}
	if config.Lock.LeaseName == "" {
		config.Lock.LeaseName = "cksr-lock"
	}
	if config.Lock.Identity == "" {
		config.Lock.Identity = "cksr-instance"
	}
    if config.Lock.LockDurationSeconds == 0 {
        config.Lock.LockDurationSeconds = 300
    }

    // 设置回滚策略默认值
    if config.Rollback.Strategy == "" {
        config.Rollback.Strategy = "stop_on_error"
    }

    // 设置重试配置默认值
    if config.Retry.MaxRetries == 0 {
        config.Retry.MaxRetries = 3
    }
    if config.Retry.DelayMs == 0 {
        config.Retry.DelayMs = 100
    }

    // 设置解析器默认值
    if config.Parser.DDLParseTimeoutSeconds == 0 {
        config.Parser.DDLParseTimeoutSeconds = 60
    }

    // 验证配置
    if len(config.DatabasePairs) == 0 {
        return nil, fmt.Errorf("至少需要配置一个数据库对")
    }

    for _, pair := range config.DatabasePairs {
        if pair.Name == "" {
            return nil, fmt.Errorf("数据库对缺少 Name")
        }
        if pair.CatalogName == "" {
            return nil, fmt.Errorf("数据库对 %s 缺少 CatalogName", pair.Name)
        }
        if pair.SRTableSuffix == "" {
            return nil, fmt.Errorf("数据库对 %s 缺少 SRTableSuffix", pair.Name)
        }
        // ClickHouse 连接参数校验与默认
        if pair.ClickHouse.Host == "" {
            return nil, fmt.Errorf("数据库对 %s 的 ClickHouse.host 不能为空", pair.Name)
        }
        if pair.ClickHouse.Port <= 0 {
            return nil, fmt.Errorf("数据库对 %s 的 ClickHouse.port 必须为正整数", pair.Name)
        }
        if pair.ClickHouse.Database == "" {
            return nil, fmt.Errorf("数据库对 %s 的 ClickHouse.database 不能为空", pair.Name)
        }
        if pair.ClickHouse.DistributedDDLTaskTimeoutSeconds < 0 {
            return nil, fmt.Errorf("数据库对 %s 的 ClickHouse.distributed_ddl_task_timeout_seconds 不能为负数", pair.Name)
        }
        if pair.ClickHouse.ConnTimeoutSeconds < 0 || pair.ClickHouse.ReadTimeoutSeconds < 0 || pair.ClickHouse.WriteTimeoutSeconds < 0 {
            return nil, fmt.Errorf("数据库对 %s 的 ClickHouse 连接/读/写超时不能为负数", pair.Name)
        }
        // 默认值
        if pair.ClickHouse.ConnTimeoutSeconds == 0 {
            pair.ClickHouse.ConnTimeoutSeconds = 30
        }
        if pair.ClickHouse.ReadTimeoutSeconds == 0 {
            pair.ClickHouse.ReadTimeoutSeconds = 60
        }
        if pair.ClickHouse.WriteTimeoutSeconds == 0 {
            pair.ClickHouse.WriteTimeoutSeconds = 60
        }
        if pair.ClickHouse.JDBCSocketTimeoutMs < 0 || pair.ClickHouse.JDBCConnectionTimeoutMs < 0 {
            return nil, fmt.Errorf("数据库对 %s 的 ClickHouse JDBC 超时不能为负数", pair.Name)
        }
        if pair.ClickHouse.JDBCSocketTimeoutMs == 0 {
            pair.ClickHouse.JDBCSocketTimeoutMs = 300000
        }
        if pair.ClickHouse.JDBCConnectionTimeoutMs == 0 {
            pair.ClickHouse.JDBCConnectionTimeoutMs = 30000
        }

        // StarRocks 连接参数校验与默认
        if pair.StarRocks.Host == "" {
            return nil, fmt.Errorf("数据库对 %s 的 StarRocks.host 不能为空", pair.Name)
        }
        if pair.StarRocks.Port <= 0 {
            return nil, fmt.Errorf("数据库对 %s 的 StarRocks.port 必须为正整数", pair.Name)
        }
        if pair.StarRocks.Database == "" {
            return nil, fmt.Errorf("数据库对 %s 的 StarRocks.database 不能为空", pair.Name)
        }
        if pair.StarRocks.ConnTimeoutSeconds < 0 || pair.StarRocks.ReadTimeoutSeconds < 0 || pair.StarRocks.WriteTimeoutSeconds < 0 {
            return nil, fmt.Errorf("数据库对 %s 的 StarRocks 连接/读/写超时不能为负数", pair.Name)
        }
        if pair.StarRocks.ConnTimeoutSeconds == 0 {
            pair.StarRocks.ConnTimeoutSeconds = 30
        }
        if pair.StarRocks.ReadTimeoutSeconds == 0 {
            pair.StarRocks.ReadTimeoutSeconds = 60
        }
        if pair.StarRocks.WriteTimeoutSeconds == 0 {
            pair.StarRocks.WriteTimeoutSeconds = 60
        }
    }

    // 校验回滚策略取值
    switch config.Rollback.Strategy {
    case "stop_on_error", "continue_on_error":
        // ok
    default:
        return nil, fmt.Errorf("rollback.strategy 取值非法: %s (允许: stop_on_error, continue_on_error)", config.Rollback.Strategy)
    }

    // 校验解析器参数
    if config.Parser.DDLParseTimeoutSeconds <= 0 {
        return nil, fmt.Errorf("parser.ddl_parse_timeout_seconds 必须为正整数")
    }

    return &config, nil
}

// GetClickHouseDSNByIndex 根据索引获取ClickHouse连接字符串
func (c *Config) GetClickHouseDSNByIndex(index int) string {
    if index >= len(c.DatabasePairs) {
        return ""
    }
    pair := c.DatabasePairs[index]
    return fmt.Sprintf(
        "tcp://%s:%d?database=%s&username=%s&password=%s&read_timeout=%d&write_timeout=%d&timeout=%d",
        pair.ClickHouse.Host,
        pair.ClickHouse.Port,
        pair.ClickHouse.Database,
        pair.ClickHouse.Username,
        pair.ClickHouse.Password,
        pair.ClickHouse.ReadTimeoutSeconds,
        pair.ClickHouse.WriteTimeoutSeconds,
        pair.ClickHouse.ConnTimeoutSeconds,
    )
}

// GetStarRocksDSNByIndex 根据索引获取StarRocks连接字符串
func (c *Config) GetStarRocksDSNByIndex(index int) string {
    if index >= len(c.DatabasePairs) {
        return ""
    }
    pair := c.DatabasePairs[index]
    return fmt.Sprintf(
        "%s:%s@tcp(%s:%d)/%s?timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
        pair.StarRocks.Username,
        pair.StarRocks.Password,
        pair.StarRocks.Host,
        pair.StarRocks.Port,
        pair.StarRocks.Database,
        pair.StarRocks.ConnTimeoutSeconds,
        pair.StarRocks.ReadTimeoutSeconds,
        pair.StarRocks.WriteTimeoutSeconds,
    )
}

// GetClickHouseJDBCURIByIndex 根据索引获取ClickHouse JDBC连接字符串
func (c *Config) GetClickHouseJDBCURIByIndex(index int) string {
    if index >= len(c.DatabasePairs) {
        return ""
    }
    pair := c.DatabasePairs[index]
    return fmt.Sprintf(
        "jdbc:clickhouse://%s:%d/?database=%s&autoCommit=true&socket_timeout=%d&connection_timeout=%d&compress=true&allow_jdbctemplate_transactions=0",
        pair.ClickHouse.Host,
        pair.ClickHouse.HTTPPort,
        pair.ClickHouse.Database,
        pair.ClickHouse.JDBCSocketTimeoutMs,
        pair.ClickHouse.JDBCConnectionTimeoutMs,
    )
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
