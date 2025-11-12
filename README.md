# CKSR — StarRocks ClickHouse Catalog 视图构建工具

CKSR 用于在 StarRocks 中构建与维护统一视图，将本地冷热数据以一个视图进行承载：
- 热数据源自 StarRocks 原生表（在初始化时会重命名为统一后缀名）。
- 冷数据通过 StarRocks Catalog 挂载 ClickHouse 表，视图中以字段映射统一形态进行 `UNION ALL` 查询。
- 支持一次性更新（按指定分区边界）、常驻自动更新（Cron）、幂等初始化与安全回滚。


## 特性概览
- 幂等初始化：
  - 自动为 StarRocks 原生表重命名为统一后缀名（例如 `table` → `table_local_catalog`）。
  - 创建基础名视图 `table`，视图包含 SR 与 CH 两条路的统一查询。
- 一次性更新：
  - 通过 `cksr update` 为视图重建时间分界（例如 `timestamp >= 'YYYY-MM-DD HH:MM:SS'` 或 `>= <epoch_sec>`）。
  - 支持一次传入多组 `--table` 与 `--partition` 成对参数批量更新。
- 常驻自动更新：
  - 通过 `cksr auto-update` 按配置的 Cron 表达式定期更新视图分界。
  - 使用互斥锁（调试/lease 两种模式）避免与一次性更新并发冲突。
- 安全回滚：
  - 删除基础名视图并将后缀表重命名回基础名，清理初始化时的变更。
- 字段映射与类型转换：
  - 自动解析 CK 与 SR 的字段，做统一映射；SR-only 字段走默认占位策略。
- 稳健性：
  - 统一日志与日志文件、重试机制、解析超时保护；支持忽略表清单。


## 架构与工作流
- 初始化（`cksr init`）
  1) 导出 CK 表结构并为缺失列生成必要的别名列（在 CK 侧执行 `ALTER TABLE`）。
  2) 确保 StarRocks Catalog 存在（例如 `cold_catalog`）。
  3) 检测 SR 原生表：若为表则重命名为后缀名（如 `_local_catalog`），若已是视图则跳过。
  4) 基于 SR 后缀表与 CK 表（通过 Catalog）构建并执行 `CREATE VIEW base`。
- 一次性更新（`cksr update`）
  - 对指定视图生成并执行 `ALTER VIEW`，使用传入的分区值作为下界过滤（`timestamp >= 分界`）。
- 常驻更新（`cksr auto-update`）
  - 按 Cron 定期更新指定视图集的分界；运行期间持有互斥锁避免冲突。
- 回滚（`cksr rollback`）
  - 删除基础名视图；若存在后缀表，重命名回基础名。


## 配置说明（`config.json`）
核心结构见 `config/config.go`，示例参考仓库根目录 `config.json`：
- `database_pairs[]`
  - `name`：数据库对名称（用于 `--pair`）。
  - `catalog_name`：StarRocks Catalog 名称（用于访问 ClickHouse）。
  - `sr_table_suffix`：SR 表统一后缀（初始化重命名用）。
  - `clickhouse`：CK 连接信息（host/port/http_port/username/password/database/...）。
  - `starrocks`：SR 连接信息（host/port/username/password/database/...）。
- `ignore_tables[]`：需要忽略的表名列表。
- `timestamp_columns{}`：每表的时间戳列覆盖（`column` 与 `type`，type 支持 `datetime`/`date`/`bigint` 等）。
- `temp_dir`：临时目录（日志、导出等）。
- `driver_url`：ClickHouse JDBC 驱动 URL（供 Catalog 使用）。
- `log`：日志配置（是否写文件、文件路径、默认级别）。
- `view_updater.cron_expression`：自动更新器 Cron 表达式。
- `lock`：互斥锁配置（`debug_mode` 为 true 时使用虚拟锁，否则使用 K8s Lease）。
- `retry`：重试配置（次数与间隔）。
- `parser`：解析器相关配置（DDL 解析超时）。

示例（摘自本仓库）：
```
{
  "database_pairs": [
    {
      "name": "cold",
      "catalog_name": "cold_catalog",
      "sr_table_suffix": "_local_catalog",
      "clickhouse": { "host": "10.107.29.99", "port": 31032, "http_port": 30076, "username": "default", "password": "", "database": "business", ... },
      "starrocks": { "host": "10.107.29.99", "port": 30113, "username": "root", "password": "StarRocks!@2025#.", "database": "business", ... }
    }
  ],
  "ignore_tables": ["asset_infer"],
  "timestamp_columns": { "datalake_platform_log": { "column": "insertTime", "type": "datetime" }, ... },
  "temp_dir": "./temp",
  "driver_url": "file:///opt/starrocks/thirdparty/catalog/clickhouse-jdbc-0.4.6-all.jar",
  "log": { "enable_file_log": true, "log_file_path": "", "log_level": "DEBUG" },
  "view_updater": { "cron_expression": "*/10 * * * * *" },
  "parser": { "ddl_parse_timeout_seconds": 60 },
  "lock": { "debug_mode": true, "k8s_namespace": "olap", "lease_name": "cksr-lock", "identity": "cksr-instance", "lock_duration_seconds": 300 },
  "retry": { "max_retries": 20, "delay_ms": 500 }
}
```


## 安装与构建
- 环境要求：`Go >= 1.25`、`Docker`（用于导出 linux/amd64 二进制）、`mysql` 客户端、`jq`。
- 通过 Docker 导出二进制（推荐）：
  - `make export`（生成 `dist/linux-amd64/cksr`）
  - 可结合 `make run-case CASE=tests/cases/02a_update_with_data.sh` 或 `make test`
- 直接构建（本机）：
  - `go build -o cksr ./main.go`（需正确的 Go 环境与依赖）


## 使用方法
- 全局参数
  - `--config <path>`：配置文件路径；如果未提供，将尝试使用可执行文件同目录的 `config.json`。
  - `--log-level <SILENT|ERROR|WARN|INFO|DEBUG>`：覆盖配置中的日志级别。

- 初始化视图
  - `cksr init --config ./config.json`

- 一次性更新视图分界（可批量）
  - 单个：`cksr update --config ./config.json --pair cold --table datalake_platform_log --partition '2025-11-12 00:00:00'`
  - 批量：
    - `cksr update --config ./config.json --pair cold --table t1 --partition '2025-01-01 00:00:00' --table t2 --partition 1735689600`
  - 分区值格式：
    - `datetime`：必须带引号（例如 `'YYYY-MM-DD HH:MM:SS'`）。
    - `date`：必须带引号（例如 `'YYYY-MM-DD'`）。
    - `bigint`（epoch 秒）：不加引号，传数值（例如 `1731369600`）。

- 常驻自动更新器
  - `cksr auto-update --config ./config.json`
  - 按 `view_updater.cron_expression` 周期性更新；与一次性更新互斥。

- 回滚
  - `cksr rollback --config ./config.json`
  - 删除基础名视图并将后缀表重命名回基础名。


## 日志与审计
- 当 `log.enable_file_log = true` 时，日志写入 `temp/logs/cksr_YYYYMMDD_HHMMSS.log`。
- 命令运行时将设置模式前缀：`[INIT]`、`[UPDATE]`、`[ROLLBACK]`。
- 可通过 `--log-level` 快速调整详细程度。


## 测试用例与执行
- 先决条件：
  - 可访问的 StarRocks 与 ClickHouse（参数见 `config.json`）。
  - `mysql` 客户端与 `jq` 可用。
- 入口脚本：`tests/run_all.sh`（内置执行顺序）。
- 快速执行：
  - `make test`（自动导出二进制并运行所有用例）
  - 或 `CKSR_BIN=dist/linux-amd64/cksr bash tests/run_all.sh`
- 典型注意事项：
  - 在空表初始化后若插入数据，需要随后执行一次 `cksr update --pair <pair> --table <view> --partition <插入值>` 以使视图分界对齐，然后再断言行数。


## 常见问题
- `CONFIG_ERROR: 必须提供 --pair`
  - 一次性更新命令需要指定数据库对名称（`--pair`）。
- `构建ALTER VIEW SQL失败` 或 `执行ALTER VIEW语句失败`
  - 分区值格式与时间戳列类型不匹配（例如给 `datetime` 列传了未加引号的数值）。
- K8s lease 锁失败
  - 在非调试模式下运行时需在 Kubernetes 集群内，且具备创建/更新 lease 的权限。


## 目录结构速览
- `cmd/`：CLI 命令入口（`init`/`update`/`auto-update`/`rollback`）。
- `internal/`：核心执行逻辑（初始化/更新/回滚）。
- `builder/`：视图与字段映射构建器。
- `parser/`：DDL 解析。
- `retry/`：带重试的 SQL 执行。
- `tests/`：用例、辅助脚本与 fixtures。


## 许可
- 本仓库未包含许可声明；如需发布请依据企业规范补充。