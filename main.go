package main

import (
	"fmt"
	"log"
	"os"

	"cksr/builder"
	"cksr/config"
	"cksr/database"
	"cksr/fileops"
	"cksr/parser"
)

func main() {
	// 检查命令行参数
	if len(os.Args) < 2 {
		fmt.Println("使用方法: ./cksr <config.json>")
		os.Exit(1)
	}

	configPath := os.Args[1]

	// 加载配置
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 创建管理器
	dbManager := database.NewDatabaseManager(cfg)
	fileManager := fileops.NewFileManager(cfg.TempDir)

	// 执行主要工作流程
	if err := processDatabase(dbManager, fileManager, cfg); err != nil {
		log.Fatalf("处理数据库失败: %v", err)
	}

	fmt.Println("数据库处理完成!")
}

// processDatabase 处理数据库的主要流程
func processDatabase(dbManager *database.DatabaseManager, fileManager *fileops.FileManager, cfg *config.Config) error {
	// 1. 导出ClickHouse表结构
	fmt.Println("正在导出ClickHouse表结构...")
	ckSchemas, err := dbManager.ExportClickHouseTables()
	if err != nil {
		return fmt.Errorf("导出ClickHouse表结构失败: %w", err)
	}

	// 2. 导出StarRocks表结构
	fmt.Println("正在导出StarRocks表结构...")
	srSchemas, err := dbManager.ExportStarRocksTables()
	if err != nil {
		return fmt.Errorf("导出StarRocks表结构失败: %w", err)
	}

	// 3. 写入临时文件
	fmt.Println("正在写入临时文件...")
	if err := fileManager.WriteClickHouseSchemas(ckSchemas, cfg.ClickHouse.Database); err != nil {
		return fmt.Errorf("写入ClickHouse表结构失败: %w", err)
	}

	if err := fileManager.WriteStarRocksSchemas(srSchemas, cfg.StarRocks.Database); err != nil {
		return fmt.Errorf("写入StarRocks表结构失败: %w", err)
	}

	// 4. 创建StarRocks Catalog
	fmt.Println("正在创建StarRocks Catalog...")
	catalogName := "clickhouse_catalog"
	if err := dbManager.CreateStarRocksCatalog(catalogName); err != nil {
		return fmt.Errorf("创建StarRocks Catalog失败: %w", err)
	}

	// 5. 处理共同的表
	commonTables := fileManager.ListCommonTables(ckSchemas, srSchemas)
	fmt.Printf("找到%d个共同的表: %v\n", len(commonTables), commonTables)

	var alterSQLs []string
	var viewSQLs []string

	for _, tableName := range commonTables {
		fmt.Printf("正在处理表: %s\n", tableName)

		// 解析ClickHouse表结构
		ckTable, err := parseTableFromString(ckSchemas[tableName])
		if err != nil {
			return fmt.Errorf("解析ClickHouse表%s失败: %w", tableName, err)
		}

		// 解析StarRocks表结构
		srTable, err := parseTableFromString(srSchemas[tableName])
		if err != nil {
			return fmt.Errorf("解析StarRocks表%s失败: %w", tableName, err)
		}

		// 过滤掉通过add column操作新增的字段，确保后续流程的健壮性
		ckTable = filterAddedColumns(ckTable)

		// 生成ALTER TABLE和CREATE VIEW语句
		alterSQL, viewSQL, err := run(ckTable, srTable, catalogName)
		if err != nil {
			return fmt.Errorf("生成SQL语句失败: %w", err)
		}

		// 保存生成的SQL
		if err := fileManager.WriteGeneratedSQL(alterSQL, viewSQL, tableName); err != nil {
			return fmt.Errorf("写入生成的SQL失败: %w", err)
		}

		alterSQLs = append(alterSQLs, alterSQL)
		viewSQLs = append(viewSQLs, viewSQL)
	}

	// 6. 执行ALTER TABLE语句
	fmt.Println("正在执行ClickHouse ALTER TABLE语句...")
	if err := dbManager.ExecuteBatchSQL(alterSQLs, true); err != nil {
		return fmt.Errorf("执行ClickHouse ALTER TABLE语句失败: %w", err)
	}

	// 7. 执行CREATE VIEW语句
	fmt.Println("正在执行StarRocks CREATE VIEW语句...")
	if err := dbManager.ExecuteBatchSQL(viewSQLs, false); err != nil {
		return fmt.Errorf("执行StarRocks CREATE VIEW语句失败: %w", err)
	}

	return nil
}

// filterAddedColumns 过滤掉通过add column操作新增的字段
func filterAddedColumns(table parser.Table) parser.Table {
	var filteredFields []parser.Field
	for _, field := range table.Field {
		// 过滤掉通过add column新增的字段
		if !builder.IsAddedColumnByName(field.Name) {
			filteredFields = append(filteredFields, field)
		}
	}
	
	// 创建新的表结构，保持其他信息不变
	filteredTable := table
	filteredTable.Field = filteredFields
	return filteredTable
}

// parseTableFromString 从字符串解析表结构
func parseTableFromString(ddl string) (parser.Table, error) {
	return parser.ParserTableSQL(ddl), nil
}

func getParseTable(sqlPath string) (parser.Table, error) {
	s, err := os.ReadFile(sqlPath)
	if err != nil {
		return parser.Table{}, fmt.Errorf("read file failed: %s", err.Error())
	}
	t := parser.ParserTableSQL(string(s))
	return t, nil
}

func networkSecurityLog(catalogName string) (string, string, error) {
	ckTable, err := getParseTable("D:\\Users\\User\\GolandProjects\\srsql\\exportck\\local_hot\\network_security_log_local.sql")
	if err != nil {
		return "", "", err
	}
	srTable, err := getParseTable("D:\\Users\\User\\GolandProjects\\srsql\\sqlchange\\output\\hot\\network_security_log.sql")
	if err != nil {
		return "", "", err
	}
	//for _, f := range srTable.Field {
	//	fmt.Printf("name: %s, type: %s \n", f.Name, f.Type)
	//	if f.Name == "" || f.Type == "" {
	//		fmt.Println("empty!!!!!!!!!!!!!!!!!!!!!")
	//	}
	//}
	alterSql, viewSql, err := run(ckTable, srTable, catalogName)
	if err != nil {
		return "", "", err
	}
	return alterSql, viewSql, nil
}

func run(ckTable, srTable parser.Table, catalogName string) (string, string, error) {
	converters, err := builder.NewConverters(ckTable)
	if err != nil {
		return "", "", err
	}
	alterSql := builder.NewCKAddColumnsBuilder(converters, ckTable.DDL.DBName, ckTable.DDL.TableName).Build()
	viewBuilder := builder.NewBuilder(converters, srTable.Field, ckTable.DDL.DBName, ckTable.DDL.TableName, catalogName, srTable.DDL.DBName, srTable.DDL.TableName)
	view, err := viewBuilder.Build()
	if err != nil {
		return "", "", err
	}
	return alterSql, view, nil
}
