package cmd

import (
    "os"
    "os/signal"
    "syscall"
    "time"

    "cksr/logger"
    "cksr/updater"
    "github.com/spf13/cobra"
)

// NewUpdateCmd 启动常驻视图更新器
func NewUpdateCmd() *cobra.Command {
    return &cobra.Command{
        Use:   "update",
        Short: "仅启动常驻视图更新器",
        RunE: func(cmd *cobra.Command, args []string) error {
            cfg, err := loadConfigAndInitLogging(cmd)
            if err != nil {
                return err
            }
            defer logger.CloseLogFile()

            logger.Info("启动视图更新器...")
            updaterConfig := &updater.ViewUpdaterConfig{
                Enabled:        true,
                CronExpression: cfg.ViewUpdater.CronExpression,
                DebugMode:      cfg.ViewUpdater.DebugMode,
                K8sNamespace:   cfg.ViewUpdater.K8sNamespace,
                LeaseName:      cfg.ViewUpdater.LeaseName,
                Identity:       cfg.ViewUpdater.Identity,
                LockDuration:   time.Duration(cfg.ViewUpdater.LockDurationSeconds) * time.Second,
            }

            viewUpdater, err := updater.NewViewUpdater(cfg, updaterConfig)
            if err != nil {
                logger.Error("创建视图更新器失败: %v", err)
                return nil
            }

            if err := viewUpdater.Start(); err != nil {
                logger.Error("启动视图更新器失败: %v", err)
                return nil
            }

            logger.Info("视图更新器启动成功，将在后台持续运行")

            sigChan := make(chan os.Signal, 1)
            signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

            select {
            case sig := <-sigChan:
                logger.Info("接收到信号 %v，程序即将退出", sig)
                logger.Info("正在关闭视图更新器...")
                viewUpdater.Stop()
                logger.Info("视图更新器已关闭")
            }

            return nil
        },
    }
}