package createsrviewrun

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"cksr/builder"
	"cksr/config"
	"cksr/database"
	"cksr/logger"
)

type TablePair struct {
	BaseName string
	OldTable string
	NewTable string
}

type GeneratedViewSQL struct {
	BaseName string
	OldTable string
	NewTable string
	SQL      string
}

// Run 基于同库内新旧后缀表批量创建基础名视图。
func Run(cfg *config.Config, pairName, oldSuffix, newSuffix string) error {
	generatedViews, buildErr := BuildViewSQLs(cfg, pairName, oldSuffix, newSuffix)
	if buildErr != nil {
		return buildErr
	}

	pairIndex, pair, pairErr := findDatabasePair(cfg, pairName)
	if pairErr != nil {
		return pairErr
	}

	dbManager := database.NewDatabasePairManager(cfg, pairIndex)
	if err := dbManager.InitStarRocks(); err != nil {
		return fmt.Errorf("初始化StarRocks连接失败: %w", err)
	}

	srDB, err := dbManager.GetStarRocksConnection()
	if err != nil {
		return fmt.Errorf("获取StarRocks连接失败: %w", err)
	}

	for _, generatedView := range generatedViews {
		if err := dbManager.ExecuteBatchSQLWithDB(srDB, []string{generatedView.SQL}, false); err != nil {
			return fmt.Errorf("执行CREATE VIEW失败(db=%s, view=%s, oldTable=%s, newTable=%s): %w", pair.StarRocks.Database, generatedView.BaseName, generatedView.OldTable, generatedView.NewTable, err)
		}
	}

	return nil
}

// RunGen 生成创建视图 SQL 文件而不直接执行。
func RunGen(cfg *config.Config, pairName, oldSuffix, newSuffix, outputDir string) error {
	generatedViews, err := BuildViewSQLs(cfg, pairName, oldSuffix, newSuffix)
	if err != nil {
		return err
	}
	if strings.TrimSpace(outputDir) == "" {
		return fmt.Errorf("输出目录不能为空")
	}
	if err := writeGeneratedSQLFiles(outputDir, generatedViews); err != nil {
		return err
	}
	logger.Info("已生成%d个建视图SQL文件到目录: %s", len(generatedViews), outputDir)
	return nil
}

// BuildViewSQLs 构建所有待创建视图的 SQL，但不执行。
func BuildViewSQLs(cfg *config.Config, pairName, oldSuffix, newSuffix string) ([]GeneratedViewSQL, error) {
	oldSuffix = strings.TrimSpace(oldSuffix)
	newSuffix = strings.TrimSpace(newSuffix)
	if oldSuffix == "" || newSuffix == "" {
		return nil, fmt.Errorf("oldSuffix 与 newSuffix 不能为空")
	}
	if oldSuffix == newSuffix {
		return nil, fmt.Errorf("oldSuffix 与 newSuffix 不能相同")
	}

	pairIndex, pair, pairErr := findDatabasePair(cfg, pairName)
	if pairErr != nil {
		return nil, pairErr
	}

	dbManager := database.NewDatabasePairManager(cfg, pairIndex)
	if err := dbManager.InitStarRocks(); err != nil {
		return nil, fmt.Errorf("初始化StarRocks连接失败: %w", err)
	}

	srTableNames, err := dbManager.GetStarRocksTableNames()
	if err != nil {
		return nil, fmt.Errorf("获取StarRocks表名列表失败: %w", err)
	}
	srTypes, err := dbManager.GetStarRocksTablesTypes()
	if err != nil {
		return nil, fmt.Errorf("获取StarRocks表类型失败: %w", err)
	}

	pairs, err := findTablePairs(srTableNames, srTypes, oldSuffix, newSuffix)
	if err != nil {
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, fmt.Errorf("未找到任何符合后缀映射的新旧表，oldSuffix=%s, newSuffix=%s", oldSuffix, newSuffix)
	}

	logger.Info("找到%d组待创建视图的新旧表映射", len(pairs))
	var generatedViews []GeneratedViewSQL

	for idx, tablePair := range pairs {
		logger.Info("[%d/%d] 处理视图 %s: 旧表=%s, 新表=%s", idx+1, len(pairs), tablePair.BaseName, tablePair.OldTable, tablePair.NewTable)

		oldTable, err := dbManager.GetStarRocksTableSchema(tablePair.OldTable)
		if err != nil {
			return nil, fmt.Errorf("获取旧表schema失败(db=%s, table=%s): %w", pair.StarRocks.Database, tablePair.OldTable, err)
		}
		newTable, err := dbManager.GetStarRocksTableSchema(tablePair.NewTable)
		if err != nil {
			return nil, fmt.Errorf("获取新表schema失败(db=%s, table=%s): %w", pair.StarRocks.Database, tablePair.NewTable, err)
		}

		viewBuilder := builder.NewSRPairViewBuilder(pair.StarRocks.Database, tablePair.BaseName, oldTable, newTable)
		viewSQL, err := viewBuilder.Build()
		if err != nil {
			return nil, fmt.Errorf("构建视图SQL失败(db=%s, view=%s, oldTable=%s, newTable=%s): %w", pair.StarRocks.Database, tablePair.BaseName, tablePair.OldTable, tablePair.NewTable, err)
		}
		generatedViews = append(generatedViews, GeneratedViewSQL{
			BaseName: tablePair.BaseName,
			OldTable: tablePair.OldTable,
			NewTable: tablePair.NewTable,
			SQL:      viewSQL,
		})
	}

	return generatedViews, nil
}

func findDatabasePair(cfg *config.Config, pairName string) (int, config.DatabasePair, error) {
	if strings.TrimSpace(pairName) == "" {
		return 0, config.DatabasePair{}, fmt.Errorf("必须提供数据库对名称")
	}

	for i, pair := range cfg.DatabasePairs {
		if pair.Name == pairName {
			return i, pair, nil
		}
	}
	return 0, config.DatabasePair{}, fmt.Errorf("未找到数据库对: %s", pairName)
}

func findTablePairs(tableNames []string, tableTypes map[string]string, oldSuffix, newSuffix string) ([]TablePair, error) {
	nameSet := make(map[string]bool, len(tableNames))
	for _, tableName := range tableNames {
		nameSet[tableName] = true
	}

	var pairs []TablePair
	var candidateBases []string
	for _, tableName := range tableNames {
		if !strings.HasSuffix(tableName, newSuffix) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(tableTypes[tableName]), database.StarRocksTableTypeBaseTable) {
			continue
		}
		baseName := strings.TrimSuffix(tableName, newSuffix)
		candidateBases = append(candidateBases, baseName)

		oldTable := baseName + oldSuffix
		if !nameSet[oldTable] {
			return nil, fmt.Errorf("新表 %s 匹配不到旧表 %s", tableName, oldTable)
		}
		if !strings.EqualFold(strings.TrimSpace(tableTypes[oldTable]), database.StarRocksTableTypeBaseTable) {
			return nil, fmt.Errorf("旧表 %s 不是普通表，实际类型=%s", oldTable, tableTypes[oldTable])
		}
		if nameSet[baseName] {
			return nil, fmt.Errorf("视图目标名 %s 已存在对象，无法创建视图", baseName)
		}

		pairs = append(pairs, TablePair{
			BaseName: baseName,
			OldTable: oldTable,
			NewTable: tableName,
		})
	}

	sort.Strings(candidateBases)
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].BaseName < pairs[j].BaseName
	})

	return pairs, nil
}

func writeGeneratedSQLFiles(outputDir string, generatedViews []GeneratedViewSQL) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败(dir=%s): %w", outputDir, err)
	}

	for _, generatedView := range generatedViews {
		fileName := fmt.Sprintf("%s.sql", generatedView.BaseName)
		filePath := filepath.Join(outputDir, fileName)
		if err := os.WriteFile(filePath, []byte(generatedView.SQL), 0644); err != nil {
			return fmt.Errorf("写入SQL文件失败(file=%s, view=%s): %w", filePath, generatedView.BaseName, err)
		}
	}
	return nil
}
