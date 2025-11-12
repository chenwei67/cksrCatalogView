/*
 * @File : view_updater
 * @Date : 2025/1/27
 * @Author : Assistant
 * @Version: 1.0.0
 * @Description: 动态更新视图时间边界的协程管理器（internal/autoupdaterun）
 */

package autoupdaterun

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cksr/config"
	"cksr/database"
	"cksr/internal/common"
	"cksr/internal/updaterun"
	"cksr/lock"
	"cksr/logger"
	"cksr/retry"

	"github.com/robfig/cron/v3"
)

// ViewUpdaterConfig 视图更新器配置
type ViewUpdaterConfig struct {
	// Cron表达式，定义更新时间
	CronExpression string `json:"cron_expression"`
	// 是否启用视图更新器
	Enabled bool `json:"enabled"`
}

// ViewUpdater 视图更新器
type ViewUpdater struct {
	config      *config.Config
	updaterCfg  *ViewUpdaterConfig
	lockManager lock.LockManager
	cron        *cron.Cron
	ctx         context.Context
	cancel      context.CancelFunc
	running     bool
	mu          sync.RWMutex
}

// NewViewUpdater 创建视图更新器
func NewViewUpdater(cfg *config.Config, updaterCfg *ViewUpdaterConfig) (*ViewUpdater, error) {
	if !updaterCfg.Enabled {
		return nil, fmt.Errorf("视图更新器未启用")
	}

	// 创建锁管理器
	lockManager, err := lock.CreateLockManager(
		cfg.Lock.DebugMode,
		cfg.Lock.K8sNamespace,
		cfg.Lock.LeaseName,
		common.BuildIdentity(cfg.Lock.Identity, common.RoleUpdater),
		time.Duration(cfg.Lock.LockDurationSeconds)*time.Second,
	)
	if err != nil {
		return nil, fmt.Errorf("创建锁管理器失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ViewUpdater{
		config:      cfg,
		updaterCfg:  updaterCfg,
		lockManager: lockManager,
		cron:        cron.New(cron.WithSeconds()),
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Start 启动视图更新器
func (vu *ViewUpdater) Start() error {
	vu.mu.Lock()
	defer vu.mu.Unlock()

	if vu.running {
		return fmt.Errorf("视图更新器已在运行")
	}

	logger.Info("启动视图更新器，Cron表达式: %s", vu.updaterCfg.CronExpression)

	// 添加定时任务，cron框架会自动在协程中执行
	_, err := vu.cron.AddFunc(vu.updaterCfg.CronExpression, func() {
		if err := vu.updateAllViews(); err != nil {
			logger.Error("更新视图失败: %v", err)
		}
	})
	if err != nil {
		return fmt.Errorf("添加定时任务失败: %w", err)
	}

	vu.cron.Start()
	vu.running = true

	logger.Info("视图更新器启动成功")
	return nil
}

// Stop 停止视图更新器
func (vu *ViewUpdater) Stop() {
	vu.mu.Lock()
	defer vu.mu.Unlock()

	if !vu.running {
		return
	}

	logger.Info("正在停止视图更新器...")

	// 停止cron调度器
	vu.cron.Stop()

	// 取消上下文
	vu.cancel()

	vu.running = false
	logger.Info("视图更新器已停止")
}

// IsRunning 检查是否正在运行
func (vu *ViewUpdater) IsRunning() bool {
	vu.mu.RLock()
	defer vu.mu.RUnlock()
	return vu.running
}

// updateAllViews 更新所有视图的时间边界
func (vu *ViewUpdater) updateAllViews() error {
	logger.Info("开始更新所有视图的时间边界")

	// 获取锁
	releaseLock, err := vu.lockManager.AcquireLock(vu.ctx)
	if err != nil {
		return fmt.Errorf("获取锁失败: %w", err)
	}
	defer releaseLock()

	logger.Info("成功获取锁，开始更新视图")

	// 遍历所有数据库对
	for i, pair := range vu.config.DatabasePairs {
		logger.Info("开始更新数据库对 %s 的视图", pair.Name)

		dbManager := database.NewDatabasePairManager(vu.config, i)
		if err := vu.updateViewsForPair(dbManager, pair); err != nil {
			logger.Error("更新数据库对 %s 的视图失败: %v", pair.Name, err)
			return err
		}

		logger.Info("数据库对 %s 的视图更新完成", pair.Name)
	}

	logger.Info("所有视图时间边界更新完成")
	return nil
}

// updateViewsForPair 更新单个数据库对的视图
func (vu *ViewUpdater) updateViewsForPair(dbManager *database.DatabasePairManager, pair config.DatabasePair) error {
	// 主动初始化连接池，未初始化不允许继续
	if err := dbManager.Init(); err != nil {
		return fmt.Errorf("初始化数据库连接失败: %w", err)
	}
	// 获取StarRocks连接
	srDB, err := dbManager.GetStarRocksConnection()
	if err != nil {
		return fmt.Errorf("获取StarRocks连接失败: %w", err)
	}

	// 获取ClickHouse连接
	chDB, err := dbManager.GetClickHouseConnection()
	if err != nil {
		return fmt.Errorf("获取ClickHouse连接失败: %w", err)
	}

	// 获取所有视图
	views, err := vu.getAllViews(srDB, pair.StarRocks.Database)
	if err != nil {
		return fmt.Errorf("获取视图列表失败: %w", err)
	}

	logger.Info("找到 %d 个视图需要更新", len(views))

	// 更新每个视图
	for _, viewName := range views {
		if err := vu.UpdateSingleView(srDB, chDB, dbManager, pair, viewName); err != nil {
			logger.Error("更新视图 %s 失败: %v", viewName, err)
			return err
		}
		logger.Debug("视图 %s 更新成功", viewName)
	}

	return nil
}

// getAllViews 获取所有视图名称
func (vu *ViewUpdater) getAllViews(srDB *sql.DB, database string) ([]string, error) {
	query := fmt.Sprintf("SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '%s'", database)

	rows, err := retry.QueryWithRetryDefault(srDB, vu.config, query)
	if err != nil {
		return nil, fmt.Errorf("查询视图失败: %w", err)
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var viewName string
		if err := rows.Scan(&viewName); err != nil {
			return nil, fmt.Errorf("扫描视图名称失败: %w", err)
		}
		views = append(views, viewName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历视图列表失败: %w", err)
	}

	return views, nil
}

// UpdateSingleView 更新单个视图的时间边界
func (vu *ViewUpdater) UpdateSingleView(srDB, chDB *sql.DB, dbManager *database.DatabasePairManager, pair config.DatabasePair, viewName string) error {
	// 自动更新：自行计算时间边界，然后委托一次性更新库执行
	srTableName := vu.getStarRocksTableNameFromView(viewName, pair)

	// 时间戳列名和类型解析（优先SR表名，其次视图名，最后默认）
	tsCol := "recordTimestamp"
	tsType := "bigint"
	if vu.config != nil && vu.config.TimestampColumns != nil {
		if cfg, ok := vu.config.TimestampColumns[srTableName]; ok {
			if cfg.Column != "" {
				tsCol = cfg.Column
			}
			if cfg.Type != "" {
				tsType = cfg.Type
			}
		} else if cfg, ok := vu.config.TimestampColumns[viewName]; ok {
			if cfg.Column != "" {
				tsCol = cfg.Column
			}
			if cfg.Type != "" {
				tsType = cfg.Type
			}
		}
	}

	// 查询最小时间戳
	minQuery := fmt.Sprintf("select min(%s) from %s.%s", tsCol, pair.StarRocks.Database, srTableName)
	var partStr string
	switch strings.ToLower(tsType) {
	case "datetime", "date":
		var nullable *string
		if err := retry.QueryRowAndScanWithRetryDefault(srDB, vu.config, minQuery, []interface{}{&nullable}); err != nil {
			return fmt.Errorf("查询最小时间戳失败: %w", err)
		}
		if nullable == nil || *nullable == "" {
			// 默认最大值
			if strings.ToLower(tsType) == "date" {
				partStr = "'9999-12-31'"
			} else {
				partStr = "'9999-12-31 23:59:59'"
			}
		} else {
			// 加引号
			if strings.HasPrefix(*nullable, "'") {
				partStr = *nullable
			} else {
				partStr = "'" + *nullable + "'"
			}
		}
	case "bigint":
		var nullable *int64
		if err := retry.QueryRowAndScanWithRetryDefault(srDB, vu.config, minQuery, []interface{}{&nullable}); err != nil {
			return fmt.Errorf("查询最小时间戳失败: %w", err)
		}
		if nullable == nil {
			partStr = "9999999999999"
		} else {
			partStr = fmt.Sprintf("%d", *nullable)
		}
	default:
		return fmt.Errorf("不支持的时间戳类型: %s", tsType)
	}

	// 委托一次性更新库执行（显式传分区值）
	return updaterun.UpdateSingleView(vu.config, srDB, chDB, dbManager, pair, viewName, partStr, true)
}

// getStarRocksTableNameFromView 根据视图名和配置后缀生成StarRocks表名
func (vu *ViewUpdater) getStarRocksTableNameFromView(viewName string, pair config.DatabasePair) string {
	return viewName + pair.SRTableSuffix
}

// Run 统一入口：启动视图更新器并阻塞等待退出信号
func Run(cfg *config.Config) error {
	updaterConfig := &ViewUpdaterConfig{
		Enabled:        true,
		CronExpression: cfg.ViewUpdater.CronExpression,
	}

	viewUpdater, err := NewViewUpdater(cfg, updaterConfig)
	if err != nil {
		logger.Error("创建视图更新器失败: %v", err)
		return err
	}

	if err := viewUpdater.Start(); err != nil {
		logger.Error("启动视图更新器失败: %v", err)
		return err
	}

	logger.Info("视图更新器启动成功，将在后台持续运行")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info("接收到信号 %v，程序即将退出", sig)
	logger.Info("正在关闭视图更新器...")
	viewUpdater.Stop()
	logger.Info("视图更新器已关闭")
	return nil
}
