package cmd

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"cksr/logger"

	"github.com/spf13/cobra"

	mcfg "example.com/migrationLib/config"
)

// ---- CLI 统一：退出码与日志等级 ----

// ExitCode 退出码定义
type ExitCode int

const (
	ExitOK      ExitCode = 0
	ExitRuntime ExitCode = 1
	ExitConfig  ExitCode = 2
)

// ErrConfig 标识配置相关错误的哨兵错误
var ErrConfig = errors.New("CONFIG_ERROR")

// WrapConfigErr 包装配置相关错误以便统一解析退出码
func WrapConfigErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrConfig, err)
}

// ResolveExitCode 根据错误类型解析退出码
func ResolveExitCode(err error) int {
	if err == nil {
		return int(ExitOK)
	}
	if errors.Is(err, ErrConfig) {
		return int(ExitConfig)
	}
	return int(ExitRuntime)
}

// ApplyEffectiveLogLevel 统一日志等级来源优先级：CONFIG > FLAG
// 要求：
// - 当提供的日志级别非法时，必须返回错误，不允许静默降级
// - 若非法来源为配置文件，返回的错误会包装为配置错误，参与统一退出码解析
func applyEffectiveLogLevel(flagLevel string) error {
	levelStr := flagLevel
	normalized := strings.ToUpper(strings.TrimSpace(levelStr))
	switch normalized {
	case "SILENT", "ERROR", "WARN", "WARNING", "INFO", "DEBUG":
		logger.SetLogLevel(logger.ParseLogLevel(normalized))
		logger.Info("日志级别设置为: %s (来源: %s)", logger.LogLevelString(logger.GetCurrentLevel()), "cmd.log_level")
		return nil
	default:
		return fmt.Errorf("非法日志级别: %q (来源: %s)，允许值: SILENT, ERROR, WARN, INFO, DEBUG", levelStr, "cmd.log_level")
	}
}
func LoadConfigAndInitLogging(cmd *cobra.Command) (*mcfg.Config, error) {
	flagLevel, flagErr := cmd.Root().PersistentFlags().GetString("log-level")
	if flagErr != nil {
		return nil, flagErr
	}
	inlineJSON, jsonErr := cmd.Root().PersistentFlags().GetString("config-json")
	if jsonErr != nil {
		return nil, jsonErr
	}
	return LoadInlineConfigAndInitLog(flagLevel, inlineJSON)
}

func LoadInlineConfigAndInitLog(flagLevel string, inlineJSON string) (*mcfg.Config, error) {
	cfg, err := mcfg.ParseConfigBytes([]byte(inlineJSON))
	if err != nil {
		return nil, WrapConfigErr(err)
	}
	if err := applyEffectiveLogLevel(flagLevel); err != nil {
		return nil, WrapConfigErr(err)
	}
	// 忽略配置中的文件日志设置，统一使用标准输出
	log.Printf("配置加载完成，数据库对数量: %d", len(cfg.DatabasePairs))
	return cfg, nil
}
