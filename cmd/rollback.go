package cmd

import (
    "cksr/internal/rollbackrun"
    "cksr/logger"
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
            cfg, err := loadConfigAndInitLogging(cmd)
            if err != nil {
                return err
            }
            defer logger.CloseLogFile()

            logger.Info("开始执行回退操作...")
            if err := rollbackrun.Run(cfg); err != nil {
                return err
            }
            logger.Info("回退操作完成")
            return nil
        },
    }
}