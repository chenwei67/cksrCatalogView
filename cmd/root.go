package cmd

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"cksr/config"
	"cksr/logger"

	"github.com/spf13/cobra"
)

// NewRootCmd 构建根命令，注册持久化参数与子命令
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:           "cksr",
		Short:         "StarRocks ClickHouse catalog视图构建工具",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// 持久化参数（所有子命令可用）
	rootCmd.PersistentFlags().String("config", "", "配置文件路径")
	rootCmd.PersistentFlags().String("log-level", "INFO", "日志级别 (SILENT, ERROR, WARN, INFO, DEBUG)")

	// 注册子命令
    rootCmd.AddCommand(NewInitCmd())
    rootCmd.AddCommand(NewAutoUpdateCmd())
    rootCmd.AddCommand(NewUpdateCmd())
    rootCmd.AddCommand(NewRollbackCmd())

	return rootCmd
}

// loadConfigAndInitLogging 读取配置并初始化日志（不使用全局变量）
func loadConfigAndInitLogging(cmd *cobra.Command) (*config.Config, error) {
	// 读取持久化参数
	configPath, _ := cmd.Root().PersistentFlags().GetString("config")

	// 配置文件路径处理
	if strings.TrimSpace(configPath) == "" {
		execPath, err := os.Executable()
		if err != nil {
			return nil, WrapConfigErr(err)
		}
		execDir := filepath.Dir(execPath)
		configPath = filepath.Join(execDir, "config.json")
		if _, err := os.Stat(configPath); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, WrapConfigErr(errors.New("未提供配置文件参数，且默认配置文件不存在"))
			}
			return nil, WrapConfigErr(err)
		}
		logger.Info("使用默认配置文件: %s", configPath)
	} else {
		logger.Info("使用配置文件: %s", configPath)
	}

	// 加载配置
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, WrapConfigErr(err)
	}

	// 初始化文件日志（如果启用）
	if err := logger.InitFileLogging(cfg.Log.EnableFileLog, cfg.Log.LogFilePath, cfg.TempDir); err != nil {
		return nil, WrapConfigErr(err)
	}

	// 统一日志级别优先级：CONFIG > FLAG（非法值将返回错误并上抛）
	if err := ApplyEffectiveLogLevel(cmd, cfg); err != nil {
		return nil, err
	}

	// 保守提示配置载入完成
	log.Printf("配置加载完成，数据库对数量: %d", len(cfg.DatabasePairs))
	return cfg, nil
}
