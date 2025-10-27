package retry

import (
	"database/sql"
	"fmt"
	"time"

	"cksr/config"
	"cksr/logger"
)

// Config 重试配置
type Config struct {
	MaxRetries int           // 最大重试次数
	Delay      time.Duration // 重试间隔
}

// DefaultConfig 默认重试配置
var DefaultConfig = Config{
	MaxRetries: 3,
	Delay:      100 * time.Millisecond,
}

// ConfigFromAppConfig 从应用配置创建重试配置
func ConfigFromAppConfig(appConfig *config.Config) Config {
	return Config{
		MaxRetries: appConfig.Retry.MaxRetries,
		Delay:      time.Duration(appConfig.Retry.DelayMs) * time.Millisecond,
	}
}

// QueryRowAndScanWithRetry 带重试的单行查询和扫描
func QueryRowAndScanWithRetry(db *sql.DB, config Config, query string, args []interface{}, queryArgs ...interface{}) error {
	var err error
	for i := 0; i <= config.MaxRetries; i++ {
		if i > 0 {
			logger.Warn("重试查询 (第%d次): %s", i, query)
			time.Sleep(config.Delay)
		}

		row := db.QueryRow(query, queryArgs...)
		err = row.Scan(args...)
		if err == nil {
			return nil
		}

		// 如果是 sql.ErrNoRows，不需要重试
		if err == sql.ErrNoRows {
			return err
		}

		logger.Warn("查询失败 (第%d次): %v", i+1, err)
	}
	return fmt.Errorf("查询在%d次重试后仍然失败: %w", config.MaxRetries, err)
}

// QueryRowAndScanWithRetryDefault 使用默认配置的单行查询和扫描
func QueryRowAndScanWithRetryDefault(db *sql.DB, query string, args []interface{}, queryArgs ...interface{}) error {
	return QueryRowAndScanWithRetry(db, DefaultConfig, query, args, queryArgs...)
}

// QueryWithRetry 带重试的多行查询
func QueryWithRetry(db *sql.DB, config Config, query string, args ...interface{}) (*sql.Rows, error) {
	var err error
	var rows *sql.Rows
	
	for i := 0; i <= config.MaxRetries; i++ {
		if i > 0 {
			logger.Warn("重试查询 (第%d次): %s", i, query)
			time.Sleep(config.Delay)
		}

		rows, err = db.Query(query, args...)
		if err == nil {
			return rows, nil
		}

		logger.Warn("查询失败 (第%d次): %v", i+1, err)
	}
	return nil, fmt.Errorf("查询在%d次重试后仍然失败: %w", config.MaxRetries, err)
}

// QueryWithRetryDefault 使用默认配置的多行查询
func QueryWithRetryDefault(db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	return QueryWithRetry(db, DefaultConfig, query, args...)
}