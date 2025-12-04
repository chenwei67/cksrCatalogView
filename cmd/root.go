package cmd

import (
	"github.com/spf13/cobra"
)

// NewRootCmd 构建根命令，注册持久化参数与子命令
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "cksr",
		Short: "StarRocks ClickHouse catalog视图构建工具",
	}

	// 持久化参数（所有子命令可用）
	rootCmd.PersistentFlags().String("config-json", "", "配置JSON字符串")
	rootCmd.PersistentFlags().String("log-level", "INFO", "日志级别 (SILENT, ERROR, WARN, INFO, DEBUG)")

	// 注册子命令
	rootCmd.AddCommand(NewInitCmd())
	rootCmd.AddCommand(NewAutoUpdateCmd())
	rootCmd.AddCommand(NewUpdateCmd())
	rootCmd.AddCommand(NewRollbackCmd())

	return rootCmd
}
