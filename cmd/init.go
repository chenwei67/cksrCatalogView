package cmd

import (
    "fmt"
    "strings"

    "cksr/builder"
    "cksr/config"
    "cksr/database"
    "cksr/fileops"
    "cksr/logger"
    "github.com/spf13/cobra"
)

// NewInitCmd 仅初始化并创建视图
func NewInitCmd() *cobra.Command {
    return &cobra.Command{
        Use:   "init",
        Short: "仅初始化并创建视图",
        RunE: func(cmd *cobra.Command, args []string) error {
            cfg, err := loadConfigAndInitLogging(cmd)
            if err != nil {
                return err
            }
            defer logger.CloseLogFile()

            // 处理多个数据库对（创建/同步视图）
            for i, pair := range cfg.DatabasePairs {
                logger.Info("开始处理数据库对 %s (索引: %d)", pair.Name, i)

                dbManager := database.NewDatabasePairManager(cfg, i)
                fileManager := fileops.NewFileManager(cfg.TempDir)

                if err := processDatabasePair(dbManager, fileManager, cfg, pair); err != nil {
                    return fmt.Errorf("处理数据库对 %s 失败: %w", pair.Name, err)
                }

                logger.Info("数据库对 %s 处理完成", pair.Name)
            }

            logger.Info("所有数据库对处理完成 (init)")
            return nil
        },
    }
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
        catalogName = fmt.Sprintf("clickhouse_catalog_%s", pairName)
    }
    if err := dbPairManager.CreateStarRocksCatalog(catalogName); err != nil {
        return fmt.Errorf("创建StarRocks Catalog失败: %w", err)
    }

    // 3. 处理共同的表 - 重构执行顺序
    initialSrTableNames, err := dbPairManager.GetStarRocksTableNames()
    if err != nil {
        return fmt.Errorf("获取StarRocks表名列表失败: %w", err)
    }

    var ckTableNames []string
    for tableName := range ckSchemaMap {
        ckTableNames = append(ckTableNames, tableName)
    }
    logger.Debug("ClickHouse表名列表: %v", ckTableNames)
    logger.Debug("StarRocks表名列表: %v", initialSrTableNames)

    ignoreTableMap := make(map[string]bool)
    for _, ignoreTable := range cfg.IgnoreTables {
        ignoreTableMap[ignoreTable] = true
    }

    srTableMap := make(map[string]bool)
    for _, srTableName := range initialSrTableNames {
        srTableMap[srTableName] = true
    }

    commonTables := []string{}
    renamedTables := make(map[string]bool)
    for _, ckTableName := range ckTableNames {
        if ignoreTableMap[ckTableName] {
            logger.Info("忽略表: %s (在配置的忽略列表中)", ckTableName)
            continue
        }
        if srTableMap[ckTableName] {
            commonTables = append(commonTables, ckTableName)
            logger.Debug("找到共同表: %s", ckTableName)
        } else {
            suffix := pair.SRTableSuffix
            if suffix != "" {
                renamedTableName := ckTableName + suffix
                if srTableMap[renamedTableName] {
                    viewExists, err := dbPairManager.CheckStarRocksTableIsView(ckTableName)
                    if err != nil {
                        logger.Warn("检查视图 %s 是否存在失败: %v，跳过处理", ckTableName, err)
                        continue
                    }
                    if !viewExists {
                        commonTables = append(commonTables, ckTableName)
                        renamedTables[ckTableName] = true
                        logger.Info("发现已重命名但未创建视图的表: %s -> %s，加入处理队列", ckTableName, renamedTableName)
                    }
                }
            }
        }
    }

    logger.Info("找到%d个共同的表: %v", len(commonTables), commonTables)
    logger.Info("开始处理表，总共 %d 个表需要处理...", len(commonTables))
    for i, tableName := range commonTables {
        logger.Info("[%d/%d] 正在处理表: %s", i+1, len(commonTables), tableName)

        isView, err := dbPairManager.CheckStarRocksTableIsView(tableName)
        if err != nil {
            return fmt.Errorf("检查表 %s 类型失败: %v", tableName, err)
        }
        if isView {
            logger.Debug("跳过VIEW表: %s (VIEW表不需要处理)")
            continue
        }

        logger.Debug("正在解析ClickHouse表结构...")
        ckTable, err := parseTableFromString(ckSchemaMap[tableName], pair.ClickHouse.Database, tableName)
        if err != nil {
            return fmt.Errorf("解析ClickHouse表%s失败: %w", tableName, err)
        }
        logger.Debug("ClickHouse表结构解析完成")

        actualSRTableName := tableName
        if renamedTables[tableName] {
            suffix := pair.SRTableSuffix
            actualSRTableName = tableName + suffix
            logger.Info("使用已知的重命名表名获取DDL: %s -> %s", tableName, actualSRTableName)
        }

        logger.Info("步骤1: 构建并执行ClickHouse表 %s 的ALTER SQL", tableName)

        logger.Debug("开始创建字段转换器...")
        fieldConverters, err := builder.NewConverters(ckTable)
        if err != nil {
            logger.Error("创建字段转换器失败: %v", err)
            return fmt.Errorf("创建字段转换器失败: %w", err)
        }
        logger.Debug("字段转换器创建完成，创建了 %d 个字段转换器", len(fieldConverters))

        logger.Debug("开始创建ALTER builder...")
        alterBuilder := builder.NewCKAddColumnsBuilder(fieldConverters, ckTable.DDL.DBName, ckTable.DDL.TableName)
        logger.Debug("ALTER builder创建完成")

        logger.Debug("开始生成ALTER SQL...")
        alterSQL := alterBuilder.Build()
        logger.Debug("ALTER SQL生成完成，长度: %d", len(alterSQL))

        if alterSQL != "" {
            logger.Debug("=== 完整ALTER SQL语句 ===")
            logger.Debug("数据库: %s.%s", ckTable.DDL.DBName, ckTable.DDL.TableName)
            logger.Debug("SQL内容:\n%s", alterSQL)
            logger.Debug("=== ALTER SQL语句结束 ===")
            logger.Debug("执行ClickHouse ALTER TABLE语句: %s", alterSQL)
            if err := dbPairManager.ExecuteBatchSQL([]string{alterSQL}, true); err != nil {
                logger.Error("执行ClickHouse ALTER TABLE语句失败: %v", err)
                return fmt.Errorf("执行ClickHouse ALTER TABLE语句失败: %w", err)
            }
        } else {
            logger.Debug("ALTER SQL为空，无需执行ALTER操作")
        }

        logger.Info("步骤2: 处理StarRocks表 %s 名称加后缀", tableName)
        suffix := pair.SRTableSuffix
        renamedTableName := tableName
        if suffix != "" && !strings.HasSuffix(tableName, suffix) {
            if renamedTables[tableName] {
                logger.Info("表 %s 已经重命名过了，跳过重命名步骤", tableName)
                renamedTableName = tableName + suffix
            } else {
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
                    renamedTableName = newTableName
                } else {
                    logger.Debug("跳过非native表: %s (不支持ALTER TABLE RENAME操作)", tableName)
                }
            }
        }

        logger.Info("步骤3: 构建并执行StarRocks表 %s 的VIEW SQL", tableName)
        logger.Debug("正在重新获取StarRocks表DDL (重命名后表名: %s)...", renamedTableName)
        srDDLAfterRename, err := dbPairManager.GetStarRocksTableDDL(renamedTableName)
        if err != nil {
            return fmt.Errorf("获取重命名后StarRocks表%s的DDL失败: %w", renamedTableName, err)
        }
        logger.Debug("重命名后StarRocks表DDL获取完成")

        logger.Debug("正在解析重命名后的StarRocks表结构...")
        srTableAfterRename, err := parseTableFromString(srDDLAfterRename, pair.StarRocks.Database, renamedTableName)
        if err != nil {
            return fmt.Errorf("解析重命名后StarRocks表%s失败: %w", renamedTableName, err)
        }
        logger.Debug("重命名后StarRocks表结构解析完成")

        logger.Debug("开始创建VIEW builder...")
        viewBuilder := builder.NewBuilder(
            fieldConverters,
            srTableAfterRename.Field,
            ckTable.DDL.DBName, ckTable.DDL.TableName, catalogName,
            srTableAfterRename.DDL.DBName, srTableAfterRename.DDL.TableName,
            dbPairManager,
            cfg,
        )
        logger.Debug("VIEW builder创建完成")

        logger.Debug("开始生成VIEW SQL...")
        viewSQL, err := viewBuilder.Build()
        if err != nil {
            logger.Error("构建视图失败: %v", err)
            return fmt.Errorf("构建视图失败: %w", err)
        }
        logger.Debug("VIEW SQL生成完成，长度: %d", len(viewSQL))

        if viewSQL != "" {
            logger.Debug("=== 完整VIEW SQL语句 ===")
            logger.Debug("视图名: %s.%s", pair.StarRocks.Database, tableName)
            logger.Debug("SQL内容:\n%s", viewSQL)
            logger.Debug("=== VIEW SQL语句结束 ===")
            logger.Debug("执行CREATE VIEW语句: %s", viewSQL)
            if err := dbPairManager.ExecuteBatchSQL([]string{viewSQL}, false); err != nil {
                logger.Error("执行CREATE VIEW语句失败: %v", err)
                return fmt.Errorf("执行CREATE VIEW语句失败: %w", err)
            }
        } else {
            logger.Debug("VIEW SQL为空")
        }

        logger.Info("表 %s 处理完成", tableName)
    }

    logger.Info("数据库对 %s 处理完成", pairName)
    return nil
}