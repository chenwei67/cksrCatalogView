package cmd

import (
    "cksr/logger"
    "cksr/internal/updaterun"
    "github.com/spf13/cobra"
)

// NewUpdateCmd 启动常驻视图更新器
func NewUpdateCmd() *cobra.Command {
    return &cobra.Command{
        Use:   "update",
        Short: "仅启动常驻视图更新器",
        RunE: func(cmd *cobra.Command, args []string) error {
            // 设置日志模式为 UPDATE
            logger.SetLogMode(logger.ModeUpdate)
            cfg, err := loadConfigAndInitLogging(cmd)
            if err != nil {
                return err
            }
            defer logger.CloseLogFile()

            logger.Info("启动视图更新器...")
            return updaterun.Run(cfg)
        },
    }
}