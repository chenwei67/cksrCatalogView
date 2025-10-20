package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cksr/builder"
	"cksr/config"
	"cksr/database"
	"cksr/fileops"
	"cksr/logger"
	"cksr/parser"
)

func main() {
	var configPath string
	var logLevel string
	var rollback bool

	// 定义命令行参数
	flag.StringVar(&configPath, "config", "", "配置文件路径")
	flag.StringVar(&logLevel, "log-level", "INFO", "日志级别 (SILENT, ERROR, WARN, INFO, DEBUG)")
	flag.BoolVar(&rollback, "rollback", false, "执行回退操作：删除所有视图、删除SR表新增列、去掉SR表后缀、删除CK表带后缀的列")
	flag.Parse()

	// 检查环境变量LOG_LEVEL，如果设置了则优先使用
	if envLogLevel := os.Getenv("LOG_LEVEL"); envLogLevel != "" {
		logLevel = envLogLevel
	}

	// 设置日志级别
	logger.SetLogLevel(logger.ParseLogLevel(logLevel))
	logger.Info("日志级别设置为: %s", logger.LogLevelString(logger.GetCurrentLevel()))

	// 如果没有提供配置文件参数，使用默认的config.example.json
	if configPath == "" {
		// 获取程序当前目录
		execPath, err := os.Executable()
		if err != nil {
			log.Fatalf("获取程序路径失败: %v", err)
		}
		execDir := filepath.Dir(execPath)
		configPath = filepath.Join(execDir, "config.example.json")

		// 检查默认配置文件是否存在
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			log.Fatalf("未提供配置文件参数，且默认配置文件 %s 不存在", configPath)
		}

		logger.Info("使用默认配置文件: %s", configPath)
	} else {
		logger.Info("使用配置文件: %s", configPath)
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 初始化文件日志（如果配置中启用了）
	if err := logger.InitFileLogging(cfg.Log.EnableFileLog, cfg.Log.LogFilePath, cfg.TempDir); err != nil {
		log.Fatalf("初始化文件日志失败: %v", err)
	}

	// 如果配置文件中指定了日志级别，则覆盖命令行参数
	if cfg.Log.LogLevel != "" {
		logger.SetLogLevel(logger.ParseLogLevel(cfg.Log.LogLevel))
		logger.Info("从配置文件设置日志级别为: %s", logger.LogLevelString(logger.GetCurrentLevel()))
	}

	// 确保程序退出时关闭日志文件
	defer logger.CloseLogFile()

	// 如果是回退模式，执行回退操作
	if rollback {
		logger.Info("开始执行回退操作...")
		if err := ExecuteRollbackForAllPairs(cfg); err != nil {
			log.Fatalf("回退操作失败: %v", err)
		}
		log.Println("回退操作完成")
		return
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
	logger.Info("正在导出ClickHouse表结构...")
	ckSchemaMap, err := dbPairManager.ExportClickHouseTables()
	if err != nil {
		return fmt.Errorf("导出ClickHouse表结构失败: %w", err)
	}

	// 2. 创建StarRocks Catalog（使用配置中指定的catalog名称）
	logger.Info("正在创建StarRocks Catalog...")
	catalogName := pair.CatalogName
	if catalogName == "" {
		// 如果没有配置catalog名称，使用默认格式
		catalogName = fmt.Sprintf("clickhouse_catalog_%s", pairName)
	}
	if err := dbPairManager.CreateStarRocksCatalog(catalogName); err != nil {
		return fmt.Errorf("创建StarRocks Catalog失败: %w", err)
	}

	// 3. 处理共同的表 - 重构执行顺序
	// 获取StarRocks表名列表（在重命名之前）
	initialSrTableNames, err := dbPairManager.GetStarRocksTableNames()
	if err != nil {
		return fmt.Errorf("获取StarRocks表名列表失败: %w", err)
	}

	// 调试：打印ClickHouse表名列表
	var ckTableNames []string
	for tableName := range ckSchemaMap {
		ckTableNames = append(ckTableNames, tableName)
	}
	logger.Debug("ClickHouse表名列表: %v", ckTableNames)
	logger.Debug("StarRocks表名列表: %v", initialSrTableNames)

	// 构建忽略表的map，提高查找效率
	ignoreTableMap := make(map[string]bool)
	for _, ignoreTable := range cfg.IgnoreTables {
		ignoreTableMap[ignoreTable] = true
	}

	// 构建StarRocks表名的map，提高查找效率
	srTableMap := make(map[string]bool)
	for _, srTableName := range initialSrTableNames {
		srTableMap[srTableName] = true
	}

	// 找出共同的表（基于原始表名）
	commonTables := []string{}
	renamedTables := make(map[string]bool) // 记录哪些表已经重命名过了
	for _, ckTableName := range ckTableNames {
		// 检查表是否在忽略列表中
		if ignoreTableMap[ckTableName] {
			logger.Info("忽略表: %s (在配置的忽略列表中)", ckTableName)
			continue
		}

		// 检查是否为共同表（原始表名匹配）
		if srTableMap[ckTableName] {
			commonTables = append(commonTables, ckTableName)
			logger.Debug("找到共同表: %s", ckTableName)
		} else {
			// 检查是否存在已重命名但未创建视图的情况
			// 场景：程序在重命名后、创建视图前中断，重启后需要继续处理
			suffix := pair.SRTableSuffix
			if suffix != "" {
				renamedTableName := ckTableName + suffix
				if srTableMap[renamedTableName] {
					// 检查是否已存在对应的视图，如果不存在则需要处理
					viewExists, err := dbPairManager.CheckStarRocksTableIsView(ckTableName)
					if err != nil {
						logger.Warn("检查视图 %s 是否存在失败: %v，跳过处理", ckTableName, err)
						continue
					}
					if !viewExists {
						commonTables = append(commonTables, ckTableName)
						renamedTables[ckTableName] = true // 标记这个表已经重命名过了
						logger.Info("发现已重命名但未创建视图的表: %s -> %s，加入处理队列", ckTableName, renamedTableName)
					}
				}
			}
		}
	}

	logger.Info("找到%d个共同的表: %v", len(commonTables), commonTables)

	// 对每个共同的表按顺序执行所有操作
	logger.Info("开始处理表，总共 %d 个表需要处理...", len(commonTables))
	for i, tableName := range commonTables {
		logger.Info("[%d/%d] 正在处理表: %s", i+1, len(commonTables), tableName)

		// 检查StarRocks表是否为VIEW
		isView, err := dbPairManager.CheckStarRocksTableIsView(tableName)
		if err != nil {
			logger.Warn("检查表 %s 类型失败: %v，跳过处理", tableName, err)
			continue
		}

		if isView {
			logger.Debug("跳过VIEW表: %s (VIEW表不需要处理)", tableName)
			continue
		}

		// 解析ClickHouse表结构
		logger.Debug("正在解析ClickHouse表结构...")
		ckTable, err := parseTableFromString(ckSchemaMap[tableName], pair.ClickHouse.Database, tableName)
		if err != nil {
			return fmt.Errorf("解析ClickHouse表%s失败: %w", tableName, err)
		}
		logger.Debug("ClickHouse表结构解析完成")

		// 解析StarRocks表结构（直接获取DDL）
		// 如果这个表已经在前面检测到重命名过了，直接使用重命名后的表名
		actualSRTableName := tableName
		if renamedTables[tableName] {
			suffix := pair.SRTableSuffix
			actualSRTableName = tableName + suffix
			logger.Info("使用已知的重命名表名获取DDL: %s -> %s", tableName, actualSRTableName)
		}
		
		logger.Debug("正在获取StarRocks表DDL (表名: %s)...", actualSRTableName)
		srDDL, err := dbPairManager.GetStarRocksTableDDL(actualSRTableName)
		if err != nil {
			return fmt.Errorf("获取StarRocks表%s的DDL失败: %w", actualSRTableName, err)
		}
		logger.Debug("StarRocks表DDL获取完成")

		// 直接构造StarRocks表结构，避免依赖parseSchemaString的解析
		logger.Debug("正在解析StarRocks表结构...")
		srTable, err := parseTableFromString(srDDL, pair.StarRocks.Database, tableName)
		if err != nil {
			return fmt.Errorf("解析StarRocks表%s失败: %w", tableName, err)
		}
		logger.Debug("StarRocks表结构解析完成")

		// 过滤掉通过add column操作新增的字段，确保后续流程的健壮性
		logger.Debug("正在过滤字段...")
		logger.Debug("开始过滤字段 - ClickHouse表字段数量: %d", len(ckTable.Field))
		ckTable = filterAddedColumns(ckTable)
		logger.Debug("字段过滤完成 - 过滤后字段数量: %d", len(ckTable.Field))

		// 步骤1: ClickHouse表新增列的处理
		logger.Info("步骤1: 处理ClickHouse表 %s 新增列", tableName)
		alterSQL, viewSQL, err := run(ckTable, srTable, catalogName)
		if err != nil {
			logger.Error("生成SQL语句失败: %v", err)
			return fmt.Errorf("生成SQL语句失败: %w", err)
		}

		if alterSQL != "" {
			logger.Debug("执行ClickHouse ALTER TABLE语句: %s", alterSQL)
			if err := dbPairManager.ExecuteBatchSQL([]string{alterSQL}, true); err != nil {
				logger.Error("执行ClickHouse ALTER TABLE语句失败: %v", err)
				return fmt.Errorf("执行ClickHouse ALTER TABLE语句失败: %w", err)
			}
		}

		// 步骤2: StarRocks表名加后缀
		logger.Info("步骤2: 处理StarRocks表 %s 名称加后缀", tableName)
		suffix := pair.SRTableSuffix
		if suffix != "" && !strings.HasSuffix(tableName, suffix) {
			// 如果这个表已经在前面检测到重命名过了，直接跳过
			if renamedTables[tableName] {
				logger.Info("表 %s 已经重命名过了，跳过重命名步骤", tableName)
			} else {
				// 检查表是否为native表
				isNative, err := dbPairManager.CheckStarRocksTableIsNative(tableName)
				if err != nil {
					logger.Warn("检查表 %s 类型失败: %v，跳过重命名", tableName, err)
				} else if isNative {
					newTableName := tableName + suffix
					renameSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` RENAME `%s`",
						pair.StarRocks.Database, tableName, newTableName)
					logger.Debug("执行StarRocks RENAME语句: %s", renameSQL)
					if err := dbPairManager.ExecuteStarRocksSQL(renameSQL); err != nil {
						logger.Error("执行StarRocks RENAME语句失败: %v", err)
						return fmt.Errorf("执行StarRocks RENAME语句失败: %w", err)
					}
				} else {
					logger.Debug("跳过非native表: %s (不支持ALTER TABLE RENAME操作)", tableName)
				}
			}
		}

		// 步骤4: 构建StarRocks的视图
		logger.Info("步骤4: 构建StarRocks表 %s 的视图", tableName)
		if viewSQL != "" {
			logger.Debug("执行CREATE VIEW语句: %s", viewSQL)
			// 使用重试机制执行CREATE VIEW语句，因为依赖于前面的操作
			if err := dbPairManager.ExecuteBatchSQLWithRetry([]string{viewSQL}, false, 20, time.Second*3); err != nil {
				logger.Error("执行CREATE VIEW语句失败: %v", err)
				return fmt.Errorf("执行CREATE VIEW语句失败: %w", err)
			}
		}

		logger.Info("表 %s 处理完成", tableName)
	}

	logger.Info("数据库对 %s 处理完成", pairName)
	return nil
}

// filterAddedColumns 过滤掉通过add column操作新增的字段
func filterAddedColumns(table parser.Table) parser.Table {
	// 这里可以添加过滤逻辑
	// 目前直接返回原表
	return table
}

// parseTableFromString 从DDL字符串解析表结构，并设置正确的数据库名和表名
func parseTableFromString(ddl string, dbName string, tableName string) (parser.Table, error) {
	// 输出完整DDL内容用于分析
	logger.Debug("完整DDL内容:\n%s", ddl)
	logger.Debug("DDL内容结束")

	// 使用超时机制防止解析阻塞
	done := make(chan parser.Table, 1)
	go func() {
		logger.Debug("调用ParserTableSQL函数...")
		table := parser.ParserTableSQL(ddl)
		logger.Debug("ParserTableSQL函数执行完成")
		done <- table
	}()

	select {
	case table := <-done:
		logger.Debug("DDL解析成功")
		// 强制设置正确的数据库名和表名，避免依赖DDL中的解析结果
		if dbName != "" {
			table.DDL.DBName = dbName
		}
		if tableName != "" {
			table.DDL.TableName = tableName
		}
		logger.Debug("设置数据库名: %s, 表名: %s", dbName, tableName)
		return table, nil
	case <-time.After(60 * time.Second):
		logger.Warn("DDL解析超时 (60秒)")
		return parser.Table{}, fmt.Errorf("DDL解析超时")
	}
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
	logger.Debug("run函数开始 - catalogName: %s", catalogName)
	logger.Debug("ClickHouse表字段数量: %d", len(ckTable.Field))
	logger.Debug("StarRocks表字段数量: %d", len(srTable.Field))

	// 生成字段转换器
	logger.Debug("开始创建字段转换器...")
	fieldConverters, err := builder.NewConverters(ckTable)
	if err != nil {
		logger.Error("创建字段转换器失败: %v", err)
		return "", "", fmt.Errorf("failed to create field converters: %w", err)
	}
	logger.Debug("字段转换器创建完成，创建了 %d 个字段转换器", len(fieldConverters))

	// 使用builder生成SQL语句
	logger.Debug("开始创建ALTER builder...")
	alterBuilder := builder.NewCKAddColumnsBuilder(fieldConverters, ckTable.DDL.DBName, ckTable.DDL.TableName)
	logger.Debug("ALTER builder创建完成")

	logger.Debug("开始创建VIEW builder...")
	viewBuilder := builder.NewBuilder(
		fieldConverters,
		srTable.Field,
		ckTable.DDL.DBName, ckTable.DDL.TableName, catalogName,
		srTable.DDL.DBName, srTable.DDL.TableName,
	)
	logger.Debug("VIEW builder创建完成")

	// 生成ALTER和VIEW语句
	logger.Debug("开始生成ALTER SQL...")
	alterSql := alterBuilder.Build()
	logger.Debug("ALTER SQL生成完成，长度: %d", len(alterSql))

	// 添加完整ALTER SQL语句的调试输出
	if alterSql != "" {
		logger.Debug("=== 完整ALTER SQL语句 ===")
		logger.Debug("数据库: %s.%s", ckTable.DDL.DBName, ckTable.DDL.TableName)
		logger.Debug("SQL内容:\n%s", alterSql)
		logger.Debug("=== ALTER SQL语句结束 ===")
	} else {
		logger.Debug("ALTER SQL为空，无需执行ALTER操作")
	}

	logger.Debug("开始生成VIEW SQL...")
	view, err := viewBuilder.Build()
	if err != nil {
		logger.Error("构建视图失败: %v", err)
		return "", "", fmt.Errorf("failed to build view: %w", err)
	}
	logger.Debug("VIEW SQL生成完成，长度: %d", len(view))

	// 添加完整VIEW SQL语句的调试输出
	if view != "" {
		logger.Debug("=== 完整VIEW SQL语句 ===")
		logger.Debug("视图名: %s.%s", srTable.DDL.DBName, ckTable.DDL.TableName)
		logger.Debug("SQL内容:\n%s", view)
		logger.Debug("=== VIEW SQL语句结束 ===")
	} else {
		logger.Debug("VIEW SQL为空")
	}

	return alterSql, view, nil
}
