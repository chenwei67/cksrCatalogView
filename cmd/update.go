package cmd

import (
    "errors"
    "regexp"
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

            // 解析 --table 参数，每项格式: <view_name>,<partition_value>
            if len(tableArgs) == 0 {
                return WrapConfigErr(errors.New("必须通过 --table 提供至少一个视图与分区"))
            }

            var targets []updaterun.UpdateTarget
            for _, arg := range tableArgs {
                // 仅按第一个逗号拆分，避免分区值包含逗号或空格时误拆
                idx := strings.Index(arg, ",")
                if idx <= 0 || idx >= len(arg)-1 {
                    return WrapConfigErr(errors.New("--table 参数格式错误，需为 <view>,<partition>"))
                }
                view := strings.TrimSpace(arg[:idx])
                part := strings.TrimSpace(arg[idx+1:])
                if view == "" || part == "" {
                    return WrapConfigErr(errors.New("--table 参数包含空视图或空分区值"))
                }
                // 判断是否数值（整型）
                isNumeric := regexp.MustCompile(`^-?\d+$`).MatchString(part)
                targets = append(targets, updaterun.UpdateTarget{
                    ViewName:     view,
                    Partition:    part,
                    HasPartition: true,
                    IsNumeric:    isNumeric,
                })
            }

            logger.Info("开始一次性更新 (update)，数据库对: %s，目标视图数: %d", pairName, len(targets))
            return updaterun.RunOnceForTargets(cfg, pairName, targets)
        },
    }

	cmd.Flags().StringVar(&pairName, "pair", "", "数据库对名称")
    cmd.Flags().StringArrayVar(&tableArgs, "table", nil, "目标项，格式为 <view>,<partition>，可重复传入")

	return cmd
}