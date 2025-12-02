package cmd

import (
	"errors"
	"strings"

	"cksr/database"
	"cksr/internal/updaterun"
	"cksr/logger"

	"github.com/spf13/cobra"
)

// NewUpdateCmd 一次性更新：根据数据库对与SR表名列表更新对应视图
func NewUpdateCmd() *cobra.Command {
	var pairName string
	var tableArgs []string
	var partitionArgs []string

	cmd := &cobra.Command{
		Use:   "update",
		Short: "一次性：按数据库对与SR表列表更新视图",
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

			if strings.TrimSpace(pairName) == "" {
				return WrapConfigErr(errors.New("必须提供 --pair"))
			}

			// 新参数设计：支持多组 --table <view> 与 --partition <value> 成对传入
			if len(tableArgs) == 0 || len(partitionArgs) == 0 {
				return WrapConfigErr(errors.New("必须通过 --table 与 --partition 成对提供至少一个视图"))
			}
			if len(tableArgs) != len(partitionArgs) {
				return WrapConfigErr(errors.New("--table 与 --partition 数量不一致"))
			}

			var targets []updaterun.UpdateTarget
			for i := range tableArgs {
				view := strings.TrimSpace(tableArgs[i])
				part := strings.TrimSpace(partitionArgs[i])
				if view == "" || part == "" {
					return WrapConfigErr(errors.New("存在空的视图名或分区值"))
				}
				targets = append(targets, updaterun.UpdateTarget{
					ViewName:     view,
					Partition:    part,
					HasPartition: true,
				})
			}

			logger.Info("开始一次性更新 (update)，数据库对: %s，目标视图数: %d", pairName, len(targets))
			return updaterun.RunOnceForTargets(cfg, pairName, targets)
		},
	}

	cmd.Flags().StringVar(&pairName, "pair", "", "数据库对名称")
	cmd.Flags().StringArrayVar(&tableArgs, "table", nil, "目标视图名，可重复传入，与 --partition 成对")
	cmd.Flags().StringArrayVar(&partitionArgs, "partition", nil, "分区值，可重复传入，与 --table 成对")

	return cmd
}
