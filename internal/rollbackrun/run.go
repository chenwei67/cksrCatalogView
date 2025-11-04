package rollbackrun

import (
	"context"
	"database/sql"
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

    commonTables, err := rm.findCommonTables()
    if err != nil {
        return fmt.Errorf("确认共同表失败: %w", err)
    }
    logger.Info("确认到 %d 个共同表: %v", len(commonTables), commonTables)

    rm.stats = RollbackStats{PairName: rm.pair.Name, TotalTables: len(commonTables)}

    // 针对每个共同表执行：先删视图，再改表名，最后删CK列
    for idx, table := range commonTables {
        logger.Info("[%d/%d] 回退表: %s", idx+1, len(commonTables), table)
        if err := rm.executeRollbackForTable(table); err != nil {
            // 现场打印失败信息
            var fr *FailureRecord
            if errors.As(err, &fr) {
                rm.stats.Failed = append(rm.stats.Failed, FailureRecord{Table: fr.Table, Step: fr.Step, Err: fr.Err})
            } else {
                rm.stats.Failed = append(rm.stats.Failed, FailureRecord{Table: table, Step: StepUnknown, Err: err})
            }
            if rm.cfg.Rollback.Strategy == "continue_on_error" {
                logger.Error("表 %s 回退失败(继续下一表): %v", table, err)
                continue
            }
            return fmt.Errorf("表 %s 回退失败: %w", table, err)
        }
        rm.stats.SuccessTables++
    }

    logger.Info("数据库对 %s 两阶段回退完成", rm.pair.Name)
    return nil
}

// dropCatalog 删除StarRocks中的Catalog
// 两阶段第一步：确认共同表
func (rm *RollbackManager) findCommonTables() ([]string, error) {
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

    var commonTables []string
    for table := range ckSchemaMap {
        if ignore[table] {
            logger.Info("忽略表: %s (在配置的忽略列表中)", table)
            continue
        }

        suffixed := table + suffix
        _, srSuffixedExists := srTableSet[suffixed]
        _, srBaseExists := srTableSet[table]
        srBaseType := srTypes[table]
        srBaseIsView := srBaseType == "VIEW"

        // CK是否有新增列
        ckHasAdded := false
        if cols, ok := ckColsMap[table]; ok {
            for _, c := range cols {
                if builder.IsAddedColumnByName(c) {
                    ckHasAdded = true
                    break
                }
            }
        }

        // 正常完成态或部分态都认定为共同表
        if srSuffixedExists && srBaseExists && srBaseIsView {
            commonTables = append(commonTables, table)
            continue
        }

        // 情形1：SR已重命名但未创建视图
        if srSuffixedExists && !srBaseIsView {
            commonTables = append(commonTables, table)
            continue
        }

        // 情形2：SR未重命名但CK已添加列
        if !srSuffixedExists && srBaseExists && !srBaseIsView && ckHasAdded {
            commonTables = append(commonTables, table)
            continue
        }
        // 情形3/4：不加入共同表
    }

    return commonTables, nil
}

// dropAllViews 删除StarRocks中的所有视图
// 针对单表执行回退序列：删视图 -> 去后缀 -> 删CK列
func (rm *RollbackManager) executeRollbackForTable(baseTable string) error {
    srDB := rm.pair.StarRocks.Database
    ckDB := rm.pair.ClickHouse.Database
    suffix := rm.pair.SRTableSuffix

    // 0. 预检：使用一次性类型查询同时判断存在性与类型
    renameNeeded := false
    suffixed := baseTable + suffix
    suffixedType, err := rm.dbManager.GetStarRocksTableType(suffixed)
    if err != nil && !errors.Is(err, sql.ErrNoRows){
        // 查询失败
        return &FailureRecord{Table: baseTable, Step: StepPrecheck, Err: fmt.Errorf("查询SR表类型失败(%s): %w", suffixed, err)}
    } else if errors.Is(err, sql.ErrNoRows){
		// 不存在，那么不用rename
	}

    if strings.ToUpper(suffixedType) == "BASE TABLE" {
        renameNeeded = true
        baseType, err := rm.dbManager.GetStarRocksTableType(baseTable)
        if err != nil  && !errors.Is(err, sql.ErrNoRows){
            return &FailureRecord{Table: baseTable, Step: StepPrecheck, Err: fmt.Errorf("查询SR表类型失败(%s): %w", baseTable, err)}
        } else if  errors.Is(err, sql.ErrNoRows){
			// 不存在，不需要删除view
		} else{
			baseIsView := strings.ToUpper(baseType) == "VIEW"
			if !baseIsView {
				return &FailureRecord{Table: baseTable, Step: StepPrecheck, Err: fmt.Errorf("重命名冲突：目标表 %s.%s 已存在且不是视图，停止回滚此表", srDB, baseTable)}
			}
		}
    }

    // 1. 删除VIEW（无需查询，直接使用 IF EXISTS 防御性执行）
    dropViewSQL := builder.NewRollbackBuilder(srDB, baseTable).BuildDropViewSQL()
    if err := rm.dbManager.ExecuteRollbackSQL([]string{dropViewSQL}, false); err != nil {
        return &FailureRecord{Table: baseTable, Step: StepDropView, Err: fmt.Errorf("删除视图 %s 失败: %w", baseTable, err)}
    }
    logger.Info("尝试删除视图(若存在): %s.%s", srDB, baseTable)

    // 2. 去除SR表后缀（仅在预检判定需要重命名时执行；此时视图已删除，避免冲突）
    if renameNeeded {
        renameSQL := builder.NewRollbackBuilder(srDB, suffixed).BuildRenameSRTableSQL(suffix)
        if renameSQL != "" {
            if err := rm.dbManager.ExecuteRollbackSQL([]string{renameSQL}, false); err != nil {
                return &FailureRecord{Table: baseTable, Step: StepRenameSuffix, Err: fmt.Errorf("去除后缀重命名 %s -> %s 失败: %w", suffixed, baseTable, err)}
            }
            logger.Info("已去除后缀并重命名: %s.%s -> %s", srDB, suffixed, baseTable)
        }
    }

    // 3. 删除CK表中通过 add column 新增的列
    ckCols, err := rm.dbManager.GetClickHouseTableColumns(baseTable)
    if err != nil {
        return &FailureRecord{Table: baseTable, Step: StepDropCKCols, Err: fmt.Errorf("获取CK表 %s 列信息失败: %w", baseTable, err)}
    }
    var dropCKSQLs []string
    rb := builder.NewRollbackBuilder(ckDB, baseTable)
    for _, c := range ckCols {
        if builder.IsAddedColumnByName(c) {
            dropCKSQLs = append(dropCKSQLs, rb.BuildDropCKColumnSQL(c))
        }
    }
    if len(dropCKSQLs) > 0 {
        if err := rm.dbManager.ExecuteRollbackSQL(dropCKSQLs, true); err != nil {
            return &FailureRecord{Table: baseTable, Step: StepDropCKCols, Err: fmt.Errorf("删除CK列失败(表 %s): %w", baseTable, err)}
        }
        logger.Info("已删除CK表 %s 的 %d 个新增列", baseTable, len(dropCKSQLs))
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

