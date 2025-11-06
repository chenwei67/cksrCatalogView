package cmd

import (
    "cksr/database"
    "cksr/internal/initrun"
    "cksr/logger"
    "github.com/spf13/cobra"
)

// NewInitCmd 仅初始化并创建视图
func NewInitCmd() *cobra.Command {
    return &cobra.Command{
        Use:   "init",
        Short: "初始化并创建视图",
        RunE: func(cmd *cobra.Command, args []string) error {
            // 设置日志模式为 INIT，确保后续日志带模式前缀
            logger.SetLogMode(logger.ModeInit)
            cfg, err := loadConfigAndInitLogging(cmd)
            if err != nil {
                return err
            }
            defer logger.CloseLogFile()
            // 统一在退出前关闭连接池
            defer database.CloseAll()
            return initrun.Run(cfg)
        },
    }
}