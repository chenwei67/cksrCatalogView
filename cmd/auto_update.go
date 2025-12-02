package cmd

import (
	"cksr/database"
	"cksr/internal/autoupdaterun"
	"cksr/logger"
	"github.com/spf13/cobra"
)

// NewAutoUpdateCmd 启动常驻视图更新器（auto-update）
func NewAutoUpdateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "auto-update",
		Short: "常驻：启动按Cron的视图自动更新器",
		RunE: func(cmd *cobra.Command, args []string) error {
			// 设置日志模式为 UPDATE
			logger.SetLogMode(logger.ModeUpdate)
			cfg, err := loadConfigAndInitLogging(cmd)
			if err != nil {
				return err
			}
			defer logger.CloseLogFile()
			// 统一在退出前关闭连接池
			defer database.CloseAll()

			logger.Info("启动常驻视图更新器 (auto-update)...")
			return autoupdaterun.Run(cfg)
		},
	}
}
