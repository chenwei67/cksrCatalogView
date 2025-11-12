package updaterun

import (
    "context"
    "database/sql"
    "fmt"
    "strings"
    "time"

    "cksr/builder"
    "cksr/config"
    "cksr/database"
    "cksr/internal/common"
    "cksr/lock"
    "cksr/logger"
    "cksr/retry"
)

// UpdateTarget 单次更新目标
type UpdateTarget struct {
    ViewName      string
    Partition     string
    HasPartition  bool
}

// RunOnceForTargets 一次性更新：按数据库对与视图名+分区值列表更新对应视图
func RunOnceForTargets(cfg *config.Config, pairName string, targets []UpdateTarget) error {
    // 查找数据库对索引
    var pairIndex int
    var pair config.DatabasePair
    for i, p := range cfg.DatabasePairs {
        if p.Name == pairName {
            pairIndex = i
            pair = p
            break
        }
    }
    if pairIndex < 0 {
        return fmt.Errorf("未找到数据库对: %s", pairName)
    }

    dbManager := database.NewDatabasePairManager(cfg, pairIndex)
    // 主动初始化连接池
    if err := dbManager.Init(); err != nil {
        return fmt.Errorf("初始化数据库连接失败: %w", err)
    }

    // 获取连接
    srDB, err := dbManager.GetStarRocksConnection()
    if err != nil {
        return fmt.Errorf("获取StarRocks连接失败: %w", err)
    }
    chDB, err := dbManager.GetClickHouseConnection()
    if err != nil {
        return fmt.Errorf("获取ClickHouse连接失败: %w", err)
    }

    // 创建锁管理器并获取锁，避免与常驻更新器并发冲突
    lockManager, err := lock.CreateLockManager(
        cfg.Lock.DebugMode,
        cfg.Lock.K8sNamespace,
        cfg.Lock.LeaseName,
        common.BuildIdentity(cfg.Lock.Identity, common.RoleUpdater),
        time.Duration(cfg.Lock.LockDurationSeconds)*time.Second,
    )
    if err != nil {
        return fmt.Errorf("创建锁管理器失败: %w", err)
    }
    releaseLock, err := lockManager.AcquireLock(context.Background())
    if err != nil {
        return fmt.Errorf("获取锁失败: %w", err)
    }
    defer releaseLock()

    // 逐个更新指定视图
    for _, t := range targets {
        viewName := strings.TrimSpace(t.ViewName)
        if viewName == "" {
            return fmt.Errorf("存在空的视图名")
        }
        if !t.HasPartition {
            return fmt.Errorf("视图 %s 缺少分区时间值", viewName)
        }
        if err := UpdateSingleView(cfg, srDB, chDB, dbManager, pair, viewName, t.Partition, t.HasPartition); err != nil {
            logger.Error("更新视图 %s 失败: %v", viewName, err)
            return err
        }
        logger.Info("视图 %s 更新成功", viewName)
    }

    return nil
}

// UpdateSingleView 通用更新单个视图的逻辑，可选传入分区时间值
func UpdateSingleView(cfg *config.Config, srDB, chDB *sql.DB, dbManager *database.DatabasePairManager, pair config.DatabasePair, viewName string, partitionValue string, hasPartition bool) error {
    // 一次性更新必须显式提供分区值，不允许走自动推断逻辑
    if !hasPartition {
        return fmt.Errorf("一次性更新缺少分区时间值")
    }
    // 根据视图名和配置后缀生成StarRocks表名
    srTableName := viewName + pair.SRTableSuffix

    originalTableName := viewName

    // 获取ClickHouse表结构
    ckSchemaMap, err := dbManager.ExportClickHouseTables()
    if err != nil {
        return fmt.Errorf("导出ClickHouse表结构失败: %w", err)
    }

    ckDDL, exists := ckSchemaMap[originalTableName]
    if !exists {
        return fmt.Errorf("未找到ClickHouse表 %s 的DDL", originalTableName)
    }

    // 解析ClickHouse表结构
    ckTable, err := common.ParseTableFromString(ckDDL, pair.ClickHouse.Database, originalTableName, cfg)
    if err != nil {
        return fmt.Errorf("解析ClickHouse表%s失败: %w", originalTableName, err)
    }

    // 获取StarRocks表结构
    srDDL, err := dbManager.GetStarRocksTableDDL(srTableName)
    if err != nil {
        return fmt.Errorf("获取StarRocks表%s的DDL失败: %w", srTableName, err)
    }

    // 解析StarRocks表结构
    srTable, err := common.ParseTableFromString(srDDL, pair.StarRocks.Database, srTableName, cfg)
    if err != nil {
        return fmt.Errorf("解析StarRocks表%s失败: %w", srTableName, err)
    }

    // 创建字段转换器
    fieldConverters, err := builder.NewConverters(ckTable)
    if err != nil {
        return fmt.Errorf("创建字段转换器失败: %w", err)
    }

    // 获取catalog名称
    catalogName := pair.CatalogName
    if catalogName == "" {
        return fmt.Errorf("catalog名称为空")
    }

    // 创建ViewBuilder并生成ALTER VIEW SQL
    viewBuilder := builder.NewBuilder(
        fieldConverters,
        srTable.Field,
        ckTable.DDL.DBName, ckTable.DDL.TableName, catalogName,
        srTable.DDL.DBName, srTable.DDL.TableName,
        dbManager,
        cfg,
    )

    alterViewSQL, err := viewBuilder.BuildAlterWithPartition(partitionValue)
    if err != nil {
        return fmt.Errorf("构建ALTER VIEW SQL失败: %w", err)
    }

    // 执行ALTER VIEW语句（带重试）
    if err := retry.ExecWithRetryDefault(srDB, cfg, alterViewSQL); err != nil {
        return fmt.Errorf("执行ALTER VIEW语句失败: %w", err)
    }

    logger.Info("视图 %s 已使用ALTER VIEW更新", viewName)
    return nil
}

// 无需额外上下文封装，直接使用 context.Background()