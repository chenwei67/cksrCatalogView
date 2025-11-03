package cmd

import (
	"errors"
	"fmt"
	"strings"

	"cksr/config"
	"cksr/logger"

	"github.com/spf13/cobra"
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
func ApplyEffectiveLogLevel(cmd *cobra.Command, cfg *config.Config) error {
	// 命令行参数（仅在用户显式设置时生效）；读取错误不可忽略
	flagLevel, flagErr := cmd.Root().PersistentFlags().GetString("log-level")
	if flagErr != nil {
		return WrapConfigErr(fmt.Errorf("读取 --log-level 参数失败: %w", flagErr))
	}
	flagChanged := cmd.Root().PersistentFlags().Changed("log-level")

	// 配置文件
	cfgLevel := strings.TrimSpace(cfg.Log.LogLevel)

	var levelStr string
	var source string
	var fromConfig bool
	switch {
	case cfgLevel != "":
		// 配置文件优先于命令行参数
		levelStr = cfgLevel
		source = "config.log.log_level"
		fromConfig = true
	case flagChanged:
		// 用户显式设置了参数，即使为空也要进行合法性校验，不允许静默忽略
		levelStr = flagLevel
		source = "flag --log-level"
		fromConfig = false
	default:
		levelStr = "INFO"
		source = "default"
		fromConfig = false
	}

	// 严格校验日志级别合法性（不允许忽略）
	normalized := strings.ToUpper(strings.TrimSpace(levelStr))
	switch normalized {
	case "SILENT", "ERROR", "WARN", "WARNING", "INFO", "DEBUG":
		logger.SetLogLevel(logger.ParseLogLevel(normalized))
		logger.Info("日志级别设置为: %s (来源: %s)", logger.LogLevelString(logger.GetCurrentLevel()), source)
		return nil
	default:
		err := fmt.Errorf("非法日志级别: %q (来源: %s)，允许值: SILENT, ERROR, WARN, INFO, DEBUG", levelStr, source)
		if fromConfig {
			return WrapConfigErr(err)
		}
		return err
	}
}
