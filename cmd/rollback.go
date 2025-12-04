package cmd

import (
	"cksr/internal/rollbackrun"
	"cksr/logger"

	mdb "example.com/migrationLib/database"
	"github.com/spf13/cobra"
)

// NewRollbackCmd 回滚删除视图及相关变更
func NewRollbackCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rollback",
		Short: "回滚删除视图及相关变更",
		RunE: func(cmd *cobra.Command, args []string) error {
			// 设置日志模式为 ROLLBACK
			logger.SetLogMode(logger.ModeRollback)
			cfg, err := LoadConfigAndInitLogging(cmd)
			if err != nil {
				return err
			}
			defer logger.CloseLogFile()
			// 统一在退出前关闭连接池
			defer mdb.CloseAll()

			logger.Info("开始执行回退操作...")
			if err := rollbackrun.Run(cfg); err != nil {
				return err
			}
			logger.Info("回退操作完成")
			return nil
		},
	}
}
