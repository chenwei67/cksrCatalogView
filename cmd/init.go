package cmd

import (
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
            cfg, err := loadConfigAndInitLogging(cmd)
            if err != nil {
                return err
            }
            defer logger.CloseLogFile()
            return initrun.Run(cfg)
        },
    }
}