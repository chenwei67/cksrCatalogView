package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"cksr/builder"
	"cksr/config"
	"cksr/database"
	"cksr/fileops"
	"cksr/parser"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("请提供配置文件路径")
	}

	configPath := os.Args[1]
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 处理多个数据库对
	for i, pair := range cfg.DatabasePairs {
		log.Printf("开始处理数据库对 %s (索引: %d)", pair.Name, i)

		dbManager := database.NewDatabasePairManager(cfg, i)
		fileManager := fileops.NewFileManager(cfg.TempDir)

		if err := processDatabasePair(dbManager, fileManager, cfg, pair); err != nil {
			log.Printf("处理数据库对 %s 失败: %v", pair.Name, err)
			continue
		}

		log.Printf("数据库对 %s 处理完成", pair.Name)
	}

	log.Println("所有数据库对处理完成")
}

// processDatabasePair 处理单个数据库对的完整流程
func processDatabasePair(dbPairManager *database.DatabasePairManager, fileManager *fileops.FileManager, cfg *config.Config, pair config.DatabasePair) error {
	pairName := pair.Name

	// 1. 导出ClickHouse表结构
	fmt.Println("正在导出ClickHouse表结构...")
	ckSchema, err := dbPairManager.ExportClickHouseTables()
	if err != nil {
		return fmt.Errorf("导出ClickHouse表结构失败: %w", err)
	}

	// 2. 导出StarRocks表结构
	fmt.Println("正在导出StarRocks表结构...")
	
	// 2.1 先获取表名列表
	srTableNames, err := dbPairManager.GetStarRocksTableNames()
	if err != nil {
		return fmt.Errorf("获取StarRocks表名列表失败: %w", err)
	}

	// 2.2 如果配置了表后缀，先执行重命名
	var renameSQLs []string
	if suffix := pair.SRTableSuffix; suffix != "" {
		fmt.Printf("正在为StarRocks表添加后缀 '%s'...\n", suffix)
		for _, tableName := range srTableNames {
			if !strings.HasSuffix(tableName, suffix) {
				newTableName := tableName + suffix
				renameSQL := fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`", 
					pair.StarRocks.Database, tableName, 
					pair.StarRocks.Database, newTableName)
				renameSQLs = append(renameSQLs, renameSQL)
			}
		}
		
		// 执行重命名
		if len(renameSQLs) > 0 {
			fmt.Println("正在执行SR表重命名语句...")
			if err := dbPairManager.ExecuteBatchSQL(renameSQLs, false); err != nil {
				return fmt.Errorf("执行SR表重命名语句失败: %w", err)
			}
		}
	}

	// 2.3 重命名后再导出DDL
	srSchema, err := dbPairManager.ExportStarRocksTables()
	if err != nil {
		return fmt.Errorf("导出StarRocks表结构失败: %w", err)
	}

	// 保存StarRocks表结构
	srSchemaMap := parseSchemaString(srSchema)
	if err := fileManager.WriteStarRocksSchemas(srSchemaMap, pairName); err != nil {
		return fmt.Errorf("写入StarRocks表结构失败: %w", err)
	}

	// 3. 创建StarRocks Catalog（使用配置中指定的catalog名称）
	fmt.Println("正在创建StarRocks Catalog...")
	catalogName := pair.CatalogName
	if catalogName == "" {
		// 如果没有配置catalog名称，使用默认格式
		catalogName = fmt.Sprintf("clickhouse_catalog_%s", pairName)
	}
	if err := dbPairManager.CreateStarRocksCatalog(catalogName); err != nil {
		return fmt.Errorf("创建StarRocks Catalog失败: %w", err)
	}

	// 6. 处理共同的表
	// 获取重命名后的StarRocks表名列表
	finalSrTableNames, err := dbPairManager.GetStarRocksTableNames()
	if err != nil {
		return fmt.Errorf("获取重命名后的StarRocks表名列表失败: %w", err)
	}

	// 构建StarRocks表名映射（重命名后的表名 -> 原始表名）
	srTableMap := make(map[string]string)
	suffix := strings.TrimSpace(pair.SRTableSuffix)
	for _, finalTableName := range finalSrTableNames {
		originalTableName := finalTableName
		if suffix != "" && strings.HasSuffix(finalTableName, suffix) {
			originalTableName = strings.TrimSuffix(finalTableName, suffix)
		}
		srTableMap[originalTableName] = finalTableName
	}

	ckSchemaMap := parseSchemaString(ckSchema)
	commonTables := []string{}
	
	// 找出共同的表（基于原始表名）
	for originalTableName := range ckSchemaMap {
		if _, exists := srTableMap[originalTableName]; exists {
			commonTables = append(commonTables, originalTableName)
		}
	}
	
	fmt.Printf("找到%d个共同的表: %v\n", len(commonTables), commonTables)

	var alterSQLs []string
	var viewSQLs []string

	for _, tableName := range commonTables {
		fmt.Printf("正在处理表: %s\n", tableName)

		// 解析ClickHouse表结构
		ckTable, err := parseTableFromString(ckSchemaMap[tableName], pair.ClickHouse.Database, tableName)
		if err != nil {
			return fmt.Errorf("解析ClickHouse表%s失败: %w", tableName, err)
		}

		// 解析StarRocks表结构（直接获取DDL）
		srTableName := srTableMap[tableName] // 获取重命名后的实际表名
		srDDL, err := dbPairManager.GetStarRocksTableDDL(srTableName)
		if err != nil {
			return fmt.Errorf("获取StarRocks表%s的DDL失败: %w", srTableName, err)
		}
		
		// 直接构造StarRocks表结构，避免依赖parseSchemaString的解析
		srTable, err := parseTableFromString(srDDL, pair.StarRocks.Database, srTableName)
		if err != nil {
			return fmt.Errorf("解析StarRocks表%s失败: %w", srTableName, err)
		}

		// 过滤掉通过add column操作新增的字段，确保后续流程的健壮性
		ckTable = filterAddedColumns(ckTable)

		// 生成ALTER TABLE和CREATE VIEW语句
		alterSQL, viewSQL, err := run(ckTable, srTable, catalogName)
		if err != nil {
			return fmt.Errorf("生成SQL语句失败: %w", err)
		}

		// 保存生成的SQL（为每个数据库对创建独立的文件）
		sqlFileName := fmt.Sprintf("%s_%s", tableName, pairName)
		if err := fileManager.WriteGeneratedSQL(alterSQL, viewSQL, sqlFileName); err != nil {
			return fmt.Errorf("写入生成的SQL失败: %w", err)
		}

		alterSQLs = append(alterSQLs, alterSQL)
		viewSQLs = append(viewSQLs, viewSQL)
	}

	// 7. 执行ALTER TABLE和CREATE VIEW语句
	fmt.Println("正在执行ALTER TABLE语句...")
	if err := dbPairManager.ExecuteBatchSQL(alterSQLs, true); err != nil {
		return fmt.Errorf("执行ALTER TABLE语句失败: %w", err)
	}

	fmt.Println("正在执行CREATE VIEW语句...")
	if err := dbPairManager.ExecuteBatchSQL(viewSQLs, false); err != nil {
		return fmt.Errorf("执行CREATE VIEW语句失败: %w", err)
	}

	fmt.Printf("数据库对 %s 处理完成\n", pairName)
	return nil
}

// parseSchemaString 将字符串格式的schema转换为map格式
func parseSchemaString(schemaStr string) map[string]string {
	result := make(map[string]string)
	statements := strings.Split(schemaStr, ";\n\n")

	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}

		// 简单的表名提取逻辑，可能需要根据实际情况调整
		lines := strings.Split(statement, "\n")
		if len(lines) > 0 {
			firstLine := strings.TrimSpace(lines[0])
			if strings.Contains(firstLine, "CREATE TABLE") {
				// 提取表名
				parts := strings.Fields(firstLine)
				for i, part := range parts {
					if strings.ToUpper(part) == "TABLE" && i+1 < len(parts) {
						tableName := strings.Trim(parts[i+1], "`\"")
						// 移除数据库前缀
						if dotIndex := strings.LastIndex(tableName, "."); dotIndex != -1 {
							tableName = tableName[dotIndex+1:]
						}
						result[tableName] = statement
						break
					}
				}
			}
		}
	}

	return result
}

// filterAddedColumns 过滤掉通过add column操作新增的字段
func filterAddedColumns(table parser.Table) parser.Table {
	// 这里可以添加过滤逻辑
	// 目前直接返回原表
	return table
}

// parseTableFromString 从DDL字符串解析表结构，并设置正确的数据库名和表名
func parseTableFromString(ddl string, dbName string, tableName string) (parser.Table, error) {
	table := parser.ParserTableSQL(ddl)
	
	// 强制设置正确的数据库名和表名，避免依赖DDL中的解析结果
	table.DDL.DBName = dbName
	table.DDL.TableName = tableName
	
	return table, nil
}

func getParseTable(sqlPath string) (parser.Table, error) {
	// 读取SQL文件并解析
	// 这里需要根据实际需求实现
	return parser.Table{}, nil
}

func networkSecurityLog(catalogName string) (string, string, error) {
	// 生成网络安全日志相关的SQL
	// 这里需要根据实际需求实现
	alterSql := fmt.Sprintf("ALTER TABLE network_security_log ADD COLUMN IF NOT EXISTS catalog_name String DEFAULT '%s'", catalogName)
	view := fmt.Sprintf(`
CREATE VIEW IF NOT EXISTS network_security_log_view AS
SELECT *
FROM %s.default.network_security_log
`, catalogName)

	return alterSql, view, nil
}

func run(ckTable, srTable parser.Table, catalogName string) (string, string, error) {
	// 生成字段转换器
	fieldConverters, err := builder.NewConverters(ckTable)
	if err != nil {
		return "", "", fmt.Errorf("failed to create field converters: %w", err)
	}

	// 使用builder生成SQL语句
	alterBuilder := builder.NewCKAddColumnsBuilder(fieldConverters, ckTable.DDL.DBName, ckTable.DDL.TableName)
	viewBuilder := builder.NewBuilder(
		fieldConverters,
		srTable.Field,
		ckTable.DDL.DBName, ckTable.DDL.TableName, catalogName,
		srTable.DDL.DBName, srTable.DDL.TableName,
	)

	// 生成ALTER和VIEW语句
	alterSql := alterBuilder.Build()
	view, err := viewBuilder.Build()
	if err != nil {
		return "", "", fmt.Errorf("failed to build view: %w", err)
	}

	return alterSql, view, nil
}
