package initrun

import (
	"fmt"
	"strings"
	"time"

	"cksr/builder"
	"cksr/config"
	"cksr/database"
	"cksr/internal/common"
	"cksr/logger"
	"cksr/parser"

	ckb "example.com/migrationLab/builder"
	ckc "example.com/migrationLab/convert"
	p2 "example.com/migrationLab/parser"
)

// TableInitPlan 单表初始化计划
type TableInitPlan struct {
	BaseTable     string // 基础表名（CK/SR共同的表名）
	SuffixedTable string // 若SR已重命名，则为加后缀的表名
	NeedRename    bool   // SR是否需要从基础名重命名为后缀名
	RenameReason  string // 决策原因：为何需要/不需要重命名
	ViewReason    string // 决策原因：为何需要/如何创建视图
}

// InitManager 初始化管理器，负责单个数据库对的完整流程
type InitManager struct {
	dbManager   *database.DatabasePairManager
	cfg         *config.Config
	pair        config.DatabasePair
	catalogName string
}

// NewInitManager 创建初始化管理器
func NewInitManager(cfg *config.Config, pairIndex int) *InitManager {
	return &InitManager{
		dbManager:   database.NewDatabasePairManager(cfg, pairIndex),
		cfg:         cfg,
		pair:        cfg.DatabasePairs[pairIndex],
		catalogName: cfg.DatabasePairs[pairIndex].CatalogName,
	}
}

// Run 处理多个数据库对（创建/同步视图）
func Run(cfg *config.Config) error {
	for i, pair := range cfg.DatabasePairs {
		logger.Info("开始处理数据库对 %s (索引: %d)", pair.Name, i)
		if err := NewInitManager(cfg, i).ExecuteInit(); err != nil {
			return fmt.Errorf("处理数据库对 %s 失败: %w", pair.Name, err)
		}
		logger.Info("数据库对 %s 处理完成", pair.Name)
	}
	logger.Info("所有数据库对处理完成 (init)")
	return nil
}

// ExecuteInit 执行单个数据库对的完整初始化流程
func (im *InitManager) ExecuteInit() error {
	// 主动初始化数据库连接（池）
	if err := im.dbManager.Init(); err != nil {
		return fmt.Errorf("初始化数据库连接失败: %w", err)
	}
	// 1) 导出 CK 表结构（重试由 database 层统一封装）
	logger.Info("正在导出ClickHouse表结构...")
	ckSchemaMap, err := im.dbManager.ExportClickHouseTables()
	if err != nil {
		return fmt.Errorf("导出ClickHouse表结构失败: %w", err)
	}

	// 2) 确保 SR Catalog 存在
	logger.Info("正在创建StarRocks Catalog...")
	if err := im.dbManager.CreateStarRocksCatalog(im.catalogName); err != nil {
		return fmt.Errorf("创建StarRocks Catalog失败: %w", err)
	}

	// 3) 获取 SR 表名列表
	srTableNames, err := im.dbManager.GetStarRocksTableNames()
	if err != nil {
		return fmt.Errorf("获取StarRocks表名列表失败: %w", err)
	}

	// 4) 生成初始化计划（共同表 + 是否需要重命名），一次性预取SR类型映射，避免循环内查询
	plans, err := im.findInitPlans(ckSchemaMap, srTableNames)
	if err != nil {
		return err
	}

	logger.Info("找到%d个共同的表待处理", len(plans))
	for idx, plan := range plans {
		logger.Info("[%d/%d] 处理表: %s", idx+1, len(plans), plan.BaseTable)
		if err := im.processTable(plan, ckSchemaMap); err != nil {
			return err
		}
		logger.Info("表 %s 处理完成", plan.BaseTable)
	}

	logger.Info("数据库对 %s 处理完成", im.pair.Name)
	return nil
}

// findInitPlans 确认共同表并生成初始化计划
func (im *InitManager) findInitPlans(ckSchemaMap map[string]string, srTableNames []string) ([]TableInitPlan, error) {
	// 统一后缀检查：一次性校验，避免在循环中重复判断
	suffix := strings.TrimSpace(im.pair.SRTableSuffix)
	if suffix == "" {
		return nil, fmt.Errorf("数据库对 %s 的 SRTableSuffix 为空，无法进行重命名与视图占位策略", im.pair.Name)
	}

	// 预取 SR 表类型映射（table_name -> table_type），避免在循环中逐表查询
	srTypes, err := im.dbManager.GetStarRocksTablesTypes()
	if err != nil {
		return nil, fmt.Errorf("查询StarRocks表类型失败: %w", err)
	}
	ignore := make(map[string]bool)
	for _, t := range im.cfg.IgnoreTables {
		ignore[t] = true
	}

	srMap := make(map[string]bool)
	for _, t := range srTableNames {
		srMap[t] = true
	}

	var ckTables []string
	for t := range ckSchemaMap {
		ckTables = append(ckTables, t)
	}
	logger.Debug("ClickHouse表名列表: %v", ckTables)
	logger.Debug("StarRocks表名列表: %v", srTableNames)

	var plans []TableInitPlan
	for _, ckTable := range ckTables {
		if ignore[ckTable] {
			logger.Info("忽略表: %s (在配置的忽略列表中)", ckTable)
			continue
		}

		// 情形 A：SR 存在基础名
		if srMap[ckTable] {
			// 区分是否已是视图（使用预取的类型映射）
			t := strings.ToUpper(srTypes[ckTable])
			if t == database.StarRocksTableTypeView {
				// 已创建视图，初始化无需处理
				logger.Debug("基础名 %s 在SR中为视图，跳过", ckTable)
				continue
			}
			// 非视图（原生表），需要重命名为后缀名以便创建视图占位
			plans = append(plans, TableInitPlan{
				BaseTable:     ckTable,
				SuffixedTable: ckTable + suffix,
				NeedRename:    true,
				RenameReason:  fmt.Sprintf("基础名存在且类型为%s，需要重命名为后缀以创建视图", t),
				ViewReason:    "重命名后创建基础名视图以承载双路查询",
			})
			continue
		}

		// 情形 B：SR 不存在基础名，但存在后缀名（已重命名），检查基础名是否已有视图
		renamed := ckTable + suffix
		if srMap[renamed] {
			// 基础名不存在或不是视图，则需要仅创建视图（已重命名）
			t := strings.ToUpper(srTypes[ckTable])
			if t != database.StarRocksTableTypeView {
				// 已完成重命名，但基础名视图尚未创建，计划仅创建视图
				logger.Info("发现已重命名但未创建视图的表: %s -> %s，加入处理队列", ckTable, renamed)
				plans = append(plans, TableInitPlan{
					BaseTable:     ckTable,
					SuffixedTable: renamed,
					NeedRename:    false,
					RenameReason:  "后缀表已存在，无需重命名",
					ViewReason:    "基础名视图尚未创建，需基于后缀表创建视图",
				})
			} else {
				logger.Debug("基础名 %s 在SR中已存在视图，跳过", ckTable)
			}
		}
	}
	return plans, nil
}

// processTable 处理单表：生成并执行 CK ALTER、SR 重命名、创建视图
func (im *InitManager) processTable(plan TableInitPlan, ckSchemaMap map[string]string) error {
	// 打印计划原因，便于审计
	if plan.NeedRename {
		logger.Info("计划：重命名并创建视图 - 表: %s，原因: %s；视图: %s", plan.BaseTable, plan.RenameReason, plan.ViewReason)
	} else {
		logger.Info("计划：仅创建视图 - 表: %s，原因: %s", plan.BaseTable, plan.ViewReason)
	}

	// 解析 CK 表结构
	logger.Debug("解析ClickHouse表结构: %s.%s", im.pair.ClickHouse.Database, plan.BaseTable)
	ckTable, err := common.ParseTableFromString(ckSchemaMap[plan.BaseTable], im.pair.ClickHouse.Database, plan.BaseTable, time.Duration(im.cfg.Parser.DDLParseTimeoutSeconds)*time.Second)
	if err != nil {
		return fmt.Errorf("解析ClickHouse表 %s 失败: %w", plan.BaseTable, err)
	}

	// 生成并执行 CK ALTER 以新增别名列（必要时）
	fieldConverters, err := ckc.NewConverters(ckTable)
	if err != nil {
		return fmt.Errorf("创建字段转换器失败(表 %s): %w", plan.BaseTable, err)
	}

	// 确定 SR 实际表名：
	// - 若需要重命名：基础名 -> 后缀名
	// - 若无需重命名（仅创建视图，SR侧已存在后缀表）：使用后缀表名
	if plan.NeedRename {
		alterBuilder := ckb.NewCKAddColumnsBuilder(fieldConverters, ckTable.DDL.DBName, ckTable.DDL.TableName, ckb.NewViewModeStrategies())
		alterSQL := alterBuilder.Build()
		if strings.TrimSpace(alterSQL) != "" {
			ckDB, errConn := im.dbManager.GetClickHouseConnection()
			if errConn != nil {
				return fmt.Errorf("获取ClickHouse连接失败: %w", errConn)
			}
			if err = im.dbManager.ExecuteBatchSQLWithDB(ckDB, []string{alterSQL}, true); err != nil {
				return fmt.Errorf("执行ClickHouse ALTER TABLE失败(表 %s): %w", plan.BaseTable, err)
			}
		}

		renameSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` RENAME `%s`", im.pair.StarRocks.Database, plan.BaseTable, plan.SuffixedTable)
		if err = im.dbManager.ExecuteStarRocksSQL(renameSQL); err != nil {
			return fmt.Errorf("执行StarRocks重命名失败(%s -> %s): %w", plan.BaseTable, plan.SuffixedTable, err)
		}
	}

	// 获取并解析 SR DDL（可能是重命名后的表名）
	srDDL, err := im.dbManager.GetStarRocksTableDDL(plan.SuffixedTable)
	if err != nil {
		return fmt.Errorf("获取StarRocks表DDL失败(%s): %w", plan.SuffixedTable, err)
	}
	srTable, err := common.ParseTableFromString(srDDL, im.pair.StarRocks.Database, plan.SuffixedTable, time.Duration(im.cfg.Parser.DDLParseTimeoutSeconds)*time.Second)
	if err != nil {
		return fmt.Errorf("解析StarRocks表失败(%s): %w", plan.SuffixedTable, err)
	}

	// 生成并执行视图 SQL
	viewBuilder := builder.NewBuilder(
		fieldConverters,
		toSRFields(srTable.Field),
		ckTable.DDL.DBName, ckTable.DDL.TableName, im.catalogName,
		srTable.DDL.DBName, srTable.DDL.TableName,
		im.dbManager,
		im.cfg,
	)
	viewSQL, err := viewBuilder.Build()
	if err != nil {
		return fmt.Errorf("构建视图失败(%s.%s): %w", im.pair.StarRocks.Database, plan.BaseTable, err)
	}
	if strings.TrimSpace(viewSQL) != "" {
		srDBConn, errConn := im.dbManager.GetStarRocksConnection()
		if errConn != nil {
			return fmt.Errorf("获取StarRocks连接失败: %w", errConn)
		}
		if err := im.dbManager.ExecuteBatchSQLWithDB(srDBConn, []string{viewSQL}, false); err != nil {
			return fmt.Errorf("执行CREATE VIEW失败(%s.%s): %w", im.pair.StarRocks.Database, plan.BaseTable, err)
		}
	}
	return nil
}

func toSRFields(fs []p2.Field) []parser.Field {
	out := make([]parser.Field, 0, len(fs))
	for _, f := range fs {
		out = append(out, parser.Field{Name: f.Name, Type: f.Type, DefaultKind: f.DefaultKind, DefaultExpr: f.DefaultExpr})
	}
	return out
}
