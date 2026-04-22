package createsrviewrun

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"cksr/builder"
	"cksr/config"
	"cksr/database"
	"cksr/logger"
	"cksr/retry"
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

type buildOptions struct {
	skipExistingViewCheck bool
}

// Run 基于同库内新旧后缀表批量创建基础名视图。
func Run(cfg *config.Config, pairName, oldSuffix, newSuffix string) error {
	generatedViews, buildErr := BuildViewSQLs(cfg, pairName, oldSuffix, newSuffix, buildOptions{})
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
	generatedViews, err := BuildViewSQLs(cfg, pairName, oldSuffix, newSuffix, buildOptions{skipExistingViewCheck: true})
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
func BuildViewSQLs(cfg *config.Config, pairName, oldSuffix, newSuffix string, options buildOptions) ([]GeneratedViewSQL, error) {
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

	pairs, err := findTablePairs(srTableNames, srTypes, oldSuffix, newSuffix, options)
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

		timestampColumn := getTimestampColumnName(cfg, tablePair.BaseName)
		timestampType := getTimestampColumnType(cfg, tablePair.BaseName)
		minTimestamp, err := findPartitionAwareMinTimestamp(dbManager, cfg, pair.StarRocks.Database, tablePair.NewTable, timestampColumn, timestampType)
		if err != nil {
			return nil, fmt.Errorf("获取新表最小时间值失败(db=%s, table=%s, column=%s): %w", pair.StarRocks.Database, tablePair.NewTable, timestampColumn, err)
		}

		viewBuilder := builder.NewSRPairViewBuilder(pair.StarRocks.Database, tablePair.BaseName, oldTable, newTable)
		viewSQL, err := viewBuilder.BuildWithTimeBoundary(timestampColumn, minTimestamp)
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

func findTablePairs(tableNames []string, tableTypes map[string]string, oldSuffix, newSuffix string, options buildOptions) ([]TablePair, error) {
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
		if !options.skipExistingViewCheck && nameSet[baseName] {
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

func findPartitionAwareMinTimestamp(dm *database.DatabasePairManager, cfg *config.Config, dbName, tableName, timestampColumn, timestampType string) (string, error) {
	db, err := dm.GetStarRocksConnection()
	if err != nil {
		return "", fmt.Errorf("获取StarRocks连接失败: %w", err)
	}

	partitions, err := getOrderedPartitionNames(db, cfg, dbName, tableName)
	if err != nil {
		return "", err
	}
	for _, partitionName := range partitions {
		minTimestamp, found, err := queryPartitionMinTimestamp(db, cfg, dbName, tableName, partitionName, timestampColumn, timestampType)
		if err != nil {
			return "", err
		}
		if found {
			logger.Info("表 %s 在分区 %s 上找到最小时间值: %s", tableName, partitionName, minTimestamp)
			return minTimestamp, nil
		}
	}

	logger.Warn("表 %s 所有分区的时间列 %s 均为NULL，使用默认最大时间值", tableName, timestampColumn)
	return getDefaultTimestampValue(timestampType)
}

func getOrderedPartitionNames(db *sql.DB, cfg *config.Config, dbName, tableName string) ([]string, error) {
	query := fmt.Sprintf("SHOW PARTITIONS FROM `%s`.`%s` ORDER BY PartitionName", dbName, tableName)
	rows, err := retry.QueryWithRetryDefault(db, cfg, query)
	if err != nil {
		return nil, fmt.Errorf("查询表分区失败(db=%s, table=%s): %w", dbName, tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("读取分区结果列失败(db=%s, table=%s): %w", dbName, tableName, err)
	}
	partitionNameIndex := -1
	for i, column := range columns {
		if strings.EqualFold(column, "PartitionName") {
			partitionNameIndex = i
			break
		}
	}
	if partitionNameIndex < 0 {
		return nil, fmt.Errorf("分区结果中缺少 PartitionName 列(db=%s, table=%s)", dbName, tableName)
	}

	var partitions []string
	for rows.Next() {
		rawValues := make([]sql.RawBytes, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range rawValues {
			scanArgs[i] = &rawValues[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("扫描分区结果失败(db=%s, table=%s): %w", dbName, tableName, err)
		}
		partitionName := strings.TrimSpace(string(rawValues[partitionNameIndex]))
		if partitionName != "" {
			partitions = append(partitions, partitionName)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历分区结果失败(db=%s, table=%s): %w", dbName, tableName, err)
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("未查询到任何分区(db=%s, table=%s)", dbName, tableName)
	}
	return partitions, nil
}

func queryPartitionMinTimestamp(db *sql.DB, cfg *config.Config, dbName, tableName, partitionName, timestampColumn, timestampType string) (string, bool, error) {
	if !isSafeIdentifier(partitionName) {
		return "", false, fmt.Errorf("分区名非法(db=%s, table=%s, partition=%s)", dbName, tableName, partitionName)
	}
	query := fmt.Sprintf("SELECT min(`%s`) FROM `%s`.`%s` PARTITION(%s)", timestampColumn, dbName, tableName, partitionName)

	switch strings.ToLower(timestampType) {
	case "datetime", "date":
		var nullableTimestamp sql.NullString
		if err := retry.QueryRowAndScanWithRetryDefault(db, cfg, query, []any{&nullableTimestamp}); err != nil {
			return "", false, fmt.Errorf("查询分区最小时间失败(db=%s, table=%s, partition=%s, column=%s): %w", dbName, tableName, partitionName, timestampColumn, err)
		}
		if !nullableTimestamp.Valid || strings.TrimSpace(nullableTimestamp.String) == "" {
			return "", false, nil
		}
		formatted, err := formatTimestampValue(nullableTimestamp.String, timestampType)
		if err != nil {
			return "", false, fmt.Errorf("格式化分区最小时间失败(db=%s, table=%s, partition=%s, column=%s): %w", dbName, tableName, partitionName, timestampColumn, err)
		}
		return formatted, true, nil
	case "bigint":
		var nullableTimestamp sql.NullInt64
		if err := retry.QueryRowAndScanWithRetryDefault(db, cfg, query, []any{&nullableTimestamp}); err != nil {
			return "", false, fmt.Errorf("查询分区最小时间失败(db=%s, table=%s, partition=%s, column=%s): %w", dbName, tableName, partitionName, timestampColumn, err)
		}
		if !nullableTimestamp.Valid {
			return "", false, nil
		}
		return strconv.FormatInt(nullableTimestamp.Int64, 10), true, nil
	default:
		return "", false, fmt.Errorf("不支持的时间戳列类型: %s", timestampType)
	}
}

func getTimestampColumnName(cfg *config.Config, tableName string) string {
	if cfg != nil && cfg.TimestampColumns != nil {
		if columnConfig, exists := cfg.TimestampColumns[tableName]; exists {
			return columnConfig.Column
		}
		for _, pair := range cfg.DatabasePairs {
			if pair.SRTableSuffix != "" && strings.HasSuffix(tableName, pair.SRTableSuffix) {
				originalTableName := strings.TrimSuffix(tableName, pair.SRTableSuffix)
				if columnConfig, exists := cfg.TimestampColumns[originalTableName]; exists {
					return columnConfig.Column
				}
			}
		}
	}
	return "recordTimestamp"
}

func getTimestampColumnType(cfg *config.Config, tableName string) string {
	if cfg != nil && cfg.TimestampColumns != nil {
		if columnConfig, exists := cfg.TimestampColumns[tableName]; exists {
			return columnConfig.Type
		}
		for _, pair := range cfg.DatabasePairs {
			if pair.SRTableSuffix != "" && strings.HasSuffix(tableName, pair.SRTableSuffix) {
				originalTableName := strings.TrimSuffix(tableName, pair.SRTableSuffix)
				if columnConfig, exists := cfg.TimestampColumns[originalTableName]; exists {
					return columnConfig.Type
				}
			}
		}
	}
	return "bigint"
}

func getDefaultTimestampValue(dataType string) (string, error) {
	switch strings.ToLower(dataType) {
	case "date":
		return "'9999-12-31'", nil
	case "datetime":
		return "'9999-12-31 23:59:59'", nil
	case "bigint":
		return "9999999999999", nil
	default:
		return "", fmt.Errorf("不支持的时间戳数据类型: %s，仅支持date、datetime、bigint", dataType)
	}
}

func formatTimestampValue(value string, dataType string) (string, error) {
	switch strings.ToLower(dataType) {
	case "date", "datetime":
		if !strings.HasPrefix(value, "'") {
			return fmt.Sprintf("'%s'", value), nil
		}
		return value, nil
	case "bigint":
		return value, nil
	default:
		return "", fmt.Errorf("不支持的时间戳数据类型: %s，仅支持date、datetime、bigint", dataType)
	}
}

func isSafeIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, r := range identifier {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return false
	}
	return true
}
