package cmd

import (
	"errors"
	"path/filepath"
	"strings"

	"cksr/database"
	"cksr/internal/createsrviewrun"
	"cksr/logger"

	"github.com/spf13/cobra"
)

// NewCreateSRViewCmd 基于 StarRocks 同库新旧两张后缀表创建基础名视图。
func NewCreateSRViewCmd() *cobra.Command {
	var pairName string
	var oldSuffix string
	var newSuffix string
	var outputDir string

	cmd := &cobra.Command{
		Use:   "create-sr-view",
		Short: "基于 StarRocks 同库新旧后缀表创建基础名视图",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger.SetLogMode(logger.ModeInit)
			cfg, err := loadConfigAndInitLogging(cmd)
			if err != nil {
				return err
			}
			defer logger.CloseLogFile()
			defer database.CloseAll()

			if strings.TrimSpace(pairName) == "" {
				return WrapConfigErr(errors.New("必须提供 --pair"))
			}
			if strings.TrimSpace(oldSuffix) == "" {
				return WrapConfigErr(errors.New("必须提供 --old-suffix"))
			}
			if strings.TrimSpace(newSuffix) == "" {
				return WrapConfigErr(errors.New("必须提供 --new-suffix"))
			}

			return createsrviewrun.Run(cfg, pairName, oldSuffix, newSuffix)
		},
	}

	cmd.Flags().StringVar(&pairName, "pair", "", "数据库对名称")
	cmd.Flags().StringVar(&oldSuffix, "old-suffix", "", "旧表后缀")
	cmd.Flags().StringVar(&newSuffix, "new-suffix", "", "新表后缀")
	cmd.AddCommand(&cobra.Command{
		Use:   "gen",
		Short: "生成建视图 SQL 文件而不直接执行",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger.SetLogMode(logger.ModeInit)
			cfg, err := loadConfigAndInitLogging(cmd)
			if err != nil {
				return err
			}
			defer logger.CloseLogFile()
			defer database.CloseAll()

			if strings.TrimSpace(pairName) == "" {
				return WrapConfigErr(errors.New("必须提供 --pair"))
			}
			if strings.TrimSpace(oldSuffix) == "" {
				return WrapConfigErr(errors.New("必须提供 --old-suffix"))
			}
			if strings.TrimSpace(newSuffix) == "" {
				return WrapConfigErr(errors.New("必须提供 --new-suffix"))
			}

			targetOutputDir := strings.TrimSpace(outputDir)
			if targetOutputDir == "" {
				targetOutputDir = filepath.Join(cfg.TempDir, "create-sr-view-sql")
			}
			return createsrviewrun.RunGen(cfg, pairName, oldSuffix, newSuffix, targetOutputDir)
		},
	})
	cmd.PersistentFlags().StringVar(&outputDir, "output-dir", "", "gen 子命令输出目录，默认 <temp_dir>/create-sr-view-sql")

	return cmd
}
