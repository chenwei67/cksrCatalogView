package rollbackrun

import (
    "context"
    "errors"
    "fmt"
    "strings"
    "time"

    "cksr/builder"
    "cksr/config"
    "cksr/database"
    "cksr/internal/common"
    "cksr/lock"
    "cksr/logger"
)

// Run 统一入口：执行回滚逻辑（与 initrun 保持一致的接口风格）
func Run(cfg *config.Config) error {
    return ExecuteRollbackForAllPairs(cfg)
}

// RollbackManager 回退管理器
type RollbackManager struct {
    dbManager *database.DatabasePairManager
    cfg       *config.Config
    pair      config.DatabasePair
    stats     RollbackStats
}

// RollbackStep 步骤枚举
type RollbackStep string

const (
    StepPrecheck     RollbackStep = "precheck"
    StepDropView     RollbackStep = "drop_view"
    StepRenameSuffix RollbackStep = "rename_suffix"
    StepDropCKCols   RollbackStep = "drop_ck_columns"
    StepUnknown      RollbackStep = "unknown"
)

// TableRollbackPlan 单表回滚计划（由发现阶段一次性生成，执行阶段直接使用）
type TableRollbackPlan struct {
    BaseTable      string   // 基础表名（不含后缀）
    SuffixedTable  string   // 后缀表名（BaseTable + suffix）
    NeedDropView   bool     // 是否需要删除基础名对应的视图
    NeedRename     bool     // 是否需要将后缀表重命名为基础名
    CanRename      bool     // 是否允许重命名（基础名不存在或为视图）
    DropViewReason string   // 决策原因：为何需要/不需要删除视图
    RenameReason   string   // 决策原因：为何需要/不需要重命名
    RenameBlockReason string // 决策原因：为何重命名被阻止
    CKAddedColumns []string // CK 中需要删除的新增列
}

// FailureRecord 失败记录（可用于库对内和全局）
type FailureRecord struct {
    Pair  string
    Table string
    Step  RollbackStep
    Err   error
}

// RollbackStats 回滚统计信息
type RollbackStats struct {
    PairName      string
    TotalTables   int
    SuccessTables int
    Failed        []FailureRecord
}

// FailureRecord 实现 error 接口，既可用于统计也可用于上抛
func (e *FailureRecord) Error() string {
    return fmt.Sprintf("表 %s 步骤 %s 失败: %v", e.Table, e.Step, e.Err)
}

// NewRollbackManager 创建回退管理器
func NewRollbackManager(cfg *config.Config, pairIndex int) *RollbackManager {
    return &RollbackManager{
        dbManager: database.NewDatabasePairManager(cfg, pairIndex),
        cfg:       cfg,
        pair:      cfg.DatabasePairs[pairIndex],
    }
}

// ExecuteRollback 执行完整的回退操作
func (rm *RollbackManager) ExecuteRollback() error {
    logger.Info("开始执行回退操作（两阶段），数据库对: %s", rm.pair.Name)
    plans, err := rm.findRollbackPlans()
    if err != nil {
        return fmt.Errorf("确认共同表及回退计划失败: %w", err)
    }
    // 打印基础表列表以便审计
    baseTables := make([]string, 0, len(plans))
    for _, p := range plans {
        baseTables = append(baseTables, p.BaseTable)
    }
    logger.Info("确认到 %d 个共同表: %v", len(plans), baseTables)

    rm.stats = RollbackStats{PairName: rm.pair.Name, TotalTables: len(plans)}

    // 针对每个共同表执行计划：先删视图，再改表名，最后删CK列
    for idx, plan := range plans {
        logger.Info("[%d/%d] 回退表: %s", idx+1, len(plans), plan.BaseTable)
        if err := rm.executeRollbackPlan(plan); err != nil {
            // 现场打印失败信息
            var fr *FailureRecord
            if errors.As(err, &fr) {
                rm.stats.Failed = append(rm.stats.Failed, FailureRecord{Table: fr.Table, Step: fr.Step, Err: fr.Err})
            } else {
                rm.stats.Failed = append(rm.stats.Failed, FailureRecord{Table: plan.BaseTable, Step: StepUnknown, Err: err})
            }
            if rm.cfg.Rollback.Strategy == "continue_on_error" {
                logger.Error("表 %s 回退失败(继续下一表): %v", plan.BaseTable, err)
                continue
            }
            return fmt.Errorf("表 %s 回退失败: %w", plan.BaseTable, err)
        }
        rm.stats.SuccessTables++
    }

    logger.Info("数据库对 %s 两阶段回退完成", rm.pair.Name)
    return nil
}

// 两阶段第一步：确认共同表并生成回退计划
func (rm *RollbackManager) findRollbackPlans() ([]TableRollbackPlan, error) {
    suffix := rm.pair.SRTableSuffix

    // 预取 SR 表列表与类型映射
    srTableNames, err := rm.dbManager.GetStarRocksTableNames()
    if err != nil {
        return nil, fmt.Errorf("获取StarRocks表列表失败: %w", err)
    }
    srTableSet := make(map[string]struct{})
    for _, n := range srTableNames {
        srTableSet[n] = struct{}{}
    }
    srTypes, err := rm.dbManager.GetStarRocksTablesTypes()
    if err != nil {
        return nil, fmt.Errorf("获取StarRocks表类型失败: %w", err)
    }

    // 预取 CK 表DDL与全库列映射
    ckSchemaMap, err := rm.dbManager.ExportClickHouseTables()
    if err != nil {
        return nil, fmt.Errorf("导出ClickHouse表结构失败: %w", err)
    }
    ckColsMap, err := rm.dbManager.GetClickHouseTablesColumns()
    if err != nil {
        return nil, fmt.Errorf("获取ClickHouse全库列失败: %w", err)
    }

    // 忽略集合
    ignore := make(map[string]bool)
    for _, t := range rm.cfg.IgnoreTables {
        ignore[t] = true
    }

    var plans []TableRollbackPlan
    for table := range ckSchemaMap {
        if ignore[table] {
            logger.Info("忽略表: %s (在配置的忽略列表中)", table)
            continue
        }

        suffixed := table + suffix
        _, srSuffixedExists := srTableSet[suffixed]
        _, srBaseExists := srTableSet[table]
        srBaseType := srTypes[table]
        srBaseIsView := strings.ToUpper(srBaseType) == "VIEW"
        suffixedType := srTypes[suffixed]

        // CK新增列清单
        var ckAddedCols []string
        if cols, ok := ckColsMap[table]; ok {
            for _, c := range cols {
                if builder.IsAddedColumnByName(c) {
                    ckAddedCols = append(ckAddedCols, c)
                }
            }
        }

        // 认定为共同表的条件与原逻辑一致
        isCommon := false
        if srSuffixedExists && srBaseExists && srBaseIsView {
            isCommon = true
        } else if srSuffixedExists && !srBaseIsView {
            isCommon = true
        } else if !srSuffixedExists && srBaseExists && !srBaseIsView && len(ckAddedCols) > 0 {
            isCommon = true
        }
        if !isCommon {
            continue
        }

        // 计划字段
        plan := TableRollbackPlan{
            BaseTable:      table,
            SuffixedTable:  suffixed,
            NeedDropView:   srBaseExists && srBaseIsView,
            NeedRename:     srSuffixedExists && strings.ToUpper(suffixedType) == "BASE TABLE",
            CanRename:      (!srBaseExists) || srBaseIsView,
            CKAddedColumns: ckAddedCols,
        }

        // 决策原因填充
        if plan.NeedDropView {
            plan.DropViewReason = fmt.Sprintf("基础表存在且为VIEW(避免与重命名目标 %s 冲突)", table)
        } else if srBaseExists && !srBaseIsView {
            plan.DropViewReason = fmt.Sprintf("基础表存在且为非视图(类型=%s)，无需删除视图", srBaseType)
        } else if !srBaseExists {
            plan.DropViewReason = "基础表不存在，无需删除视图"
        }

        if plan.NeedRename {
            plan.RenameReason = "后缀表存在且类型为BASE TABLE，需要去后缀重命名"
        } else if srSuffixedExists {
            plan.RenameReason = fmt.Sprintf("后缀表存在但类型为%s，不需要重命名", strings.ToUpper(suffixedType))
        } else {
            plan.RenameReason = "后缀表不存在，不需要重命名"
        }

        if plan.NeedRename && !plan.CanRename {
            // 基础名存在且不是视图时阻止重命名
            baseType := strings.ToUpper(srBaseType)
            plan.RenameBlockReason = fmt.Sprintf("基础表 %s.%s 已存在且为%s，禁止从 %s 重命名", rm.pair.StarRocks.Database, table, baseType, suffixed)
        }

        plans = append(plans, plan)
    }

    return plans, nil
}

// 针对单表执行回退计划：删视图 -> 去后缀 -> 删CK列
func (rm *RollbackManager) executeRollbackPlan(plan TableRollbackPlan) error {
    srDB := rm.pair.StarRocks.Database
    ckDB := rm.pair.ClickHouse.Database
    suffix := rm.pair.SRTableSuffix

    // 0. 预检：若需要重命名但不允许重命名（基础名存在且不是视图），则直接失败并不做任何破坏性操作
    if plan.NeedRename && !plan.CanRename {
        reason := plan.RenameBlockReason
        if reason == "" {
            reason = fmt.Sprintf("重命名冲突：目标表 %s.%s 已存在且不是视图", srDB, plan.BaseTable)
        }
        return &FailureRecord{Table: plan.BaseTable, Step: StepPrecheck, Err: fmt.Errorf("%s", reason)}
    }

    // 1. 删除VIEW（仅当计划指示需要删除视图时执行）
    if plan.NeedDropView {
        dropViewSQL := builder.NewRollbackBuilder(srDB, plan.BaseTable).BuildDropViewSQL()
        if err := rm.dbManager.ExecuteRollbackSQL([]string{dropViewSQL}, false); err != nil {
            return &FailureRecord{Table: plan.BaseTable, Step: StepDropView, Err: fmt.Errorf("删除视图 %s 失败(原因: %s): %w", plan.BaseTable, plan.DropViewReason, err)}
        }
        logger.Info("已删除视图(若存在): %s.%s，原因: %s", srDB, plan.BaseTable, plan.DropViewReason)
    } else {
        logger.Info("跳过删除视图: %s.%s，原因: %s", srDB, plan.BaseTable, plan.DropViewReason)
    }

    // 2. 去除SR表后缀（仅在需要且允许时执行；此时视图已删除，避免冲突）
    if plan.NeedRename {
        renameSQL := builder.NewRollbackBuilder(srDB, plan.SuffixedTable).BuildRenameSRTableSQL(suffix)
        if renameSQL != "" {
            if err := rm.dbManager.ExecuteRollbackSQL([]string{renameSQL}, false); err != nil {
                return &FailureRecord{Table: plan.BaseTable, Step: StepRenameSuffix, Err: fmt.Errorf("去除后缀重命名 %s -> %s 失败(原因: %s): %w", plan.SuffixedTable, plan.BaseTable, plan.RenameReason, err)}
            }
            logger.Info("已去除后缀并重命名: %s.%s -> %s，原因: %s", srDB, plan.SuffixedTable, plan.BaseTable, plan.RenameReason)
        }
    }

    // 3. 删除CK表中通过 add column 新增的列（使用计划中预先识别出的列名）
    if len(plan.CKAddedColumns) > 0 {
        var dropCKSQLs []string
        rb := builder.NewRollbackBuilder(ckDB, plan.BaseTable)
        for _, c := range plan.CKAddedColumns {
            dropCKSQLs = append(dropCKSQLs, rb.BuildDropCKColumnSQL(c))
        }
        if err := rm.dbManager.ExecuteRollbackSQL(dropCKSQLs, true); err != nil {
            return &FailureRecord{Table: plan.BaseTable, Step: StepDropCKCols, Err: fmt.Errorf("删除CK列失败(表 %s): %w", plan.BaseTable, err)}
        }
        logger.Info("已删除CK表 %s 的 %d 个新增列", plan.BaseTable, len(dropCKSQLs))
    }

    return nil
}

// 打印单库对的回滚统计信息（执行结束时调用一次）
func (rm *RollbackManager) printPairSummary() {
    s := rm.stats
    logger.Info("回退统计 - 数据库对: %s", s.PairName)
    logger.Info("总表: %d, 成功: %d, 失败: %d", s.TotalTables, s.SuccessTables, len(s.Failed))
    if len(s.Failed) > 0 {
        logger.Info("失败详情：")
        for i, f := range s.Failed {
            logger.Error("[%d] 表: %s, 步骤: %s, 错误: %v", i+1, f.Table, string(f.Step), f.Err)
        }
    }
}


// ExecuteRollbackForAllPairs 对所有数据库对执行回退操作
func ExecuteRollbackForAllPairs(cfg *config.Config) error {
    lockManager, err := lock.CreateLockManager(
        cfg.Lock.DebugMode,
        cfg.Lock.K8sNamespace,
        cfg.Lock.LeaseName,
        common.BuildIdentity(cfg.Lock.Identity, common.RoleRollback),
        time.Duration(cfg.Lock.LockDurationSeconds)*time.Second,
    )
    if err != nil {
        return fmt.Errorf("创建锁管理器失败: %w", err)
    }

    releaseLock, err := lockManager.AcquireLock(context.Background())
    if err != nil {
        return fmt.Errorf("获取锁失败，可能有其他操作正在进行: %w", err)
    }
    defer releaseLock()

    logger.Info("成功获取锁，开始执行回滚操作")

    // 全局统计
    totalTables := 0
    successTables := 0
    var failedAll []FailureRecord

    for i, pair := range cfg.DatabasePairs {
        logger.Info("开始回退数据库对: %s", pair.Name)

        rollbackManager := NewRollbackManager(cfg, i)
        err := rollbackManager.ExecuteRollback()
		// 汇总当前库对的统计到全局
		totalTables += rollbackManager.stats.TotalTables
		successTables += rollbackManager.stats.SuccessTables
		for _, f := range rollbackManager.stats.Failed {
			failedAll = append(failedAll, FailureRecord{Pair: pair.Name, Table: f.Table, Step: f.Step, Err: f.Err})
		}
		if  err != nil {
            logger.Error("回退数据库对 %s 失败: %v", pair.Name, err)
            return err
        }


        logger.Info("数据库对 %s 回退完成", pair.Name)
		rollbackManager.printPairSummary()
    }

    // 全局统计打印
    logger.Info("回退统计 - 全局")
    logger.Info("总表: %d, 成功: %d, 失败: %d", totalTables, successTables, len(failedAll))
    if len(failedAll) > 0 {
        logger.Info("失败详情(跨库对)：")
        for i, f := range failedAll {
            logger.Error("[%d] 库对: %s, 表: %s, 步骤: %s, 错误: %v", i+1, f.Pair, f.Table, string(f.Step), f.Err)
        }
    }
    // 第二阶段：所有数据库对处理完后，统一删除 Catalog（存在性检查）
    if err := dropAllCatalogsAfterAllPairs(cfg); err != nil {
        return err
    }
    logger.Info("所有数据库对回退与Catalog清理完成")
    return nil
}

// 在所有数据库对处理完后统一删除 Catalog（避免复用导致误删）
func dropAllCatalogsAfterAllPairs(cfg *config.Config) error {
    // 收集唯一的 Catalog 名称
    catalogSet := make(map[string]struct{})
    for _, p := range cfg.DatabasePairs {
        if p.CatalogName != "" {
            catalogSet[p.CatalogName] = struct{}{}
        }
    }
    if len(catalogSet) == 0 {
        return fmt.Errorf("未配置任何Catalog名称")
    }

    rollbackBuilder := builder.NewRollbackBuilder("", "")

    // 遍历所有数据库对，逐一检查并删除Catalog（只删一次）
    for i := range cfg.DatabasePairs {
        dm := database.NewDatabasePairManager(cfg, i)
        for name := range catalogSet {
            exists, err := dm.CheckStarRocksCatalogExists(name)
            if err != nil {
                return fmt.Errorf("检查Catalog %s 是否存在失败: %w", name, err)
            }
            if exists {
                sql := rollbackBuilder.BuildDropCatalogSQL(name)
                if err := dm.ExecuteRollbackSQL([]string{sql}, false); err != nil {
                    return fmt.Errorf("删除Catalog %s 失败: %w", name, err)
                }
                logger.Info("已删除Catalog: %s", name)
                // 删除后不再重复处理该 Catalog
                delete(catalogSet, name)
            }
        }
    }
    return nil
}

