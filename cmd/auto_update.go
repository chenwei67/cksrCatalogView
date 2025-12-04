package cmd

import (
	"cksr/internal/autoupdaterun"
	"cksr/logger"

	mdb "example.com/migrationLib/database"
	"github.com/spf13/cobra"
)

// NewAutoUpdateCmd 启动常驻视图更新器（auto-update）
func NewAutoUpdateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "auto-update",
		Short: "常驻：启动按Cron的视图自动更新器",
		RunE: func(cmd *cobra.Command, args []string) error {
			// 设置日志模式为 UPDATE
			cfg, err := LoadConfigAndInitLogging(cmd)
			if err != nil {
				return err
			}
			defer logger.CloseLogFile()
			// 统一在退出前关闭连接池
			defer mdb.CloseAll()

			logger.Info("启动常驻视图更新器 (auto-update)...")
			return autoupdaterun.Run(cfg)
		},
	}
}
