# CKSR 转测文档

## 1. 范围与目标
- 范围：`cksr` CLI 工具，用于在 StarRocks 中以视图形式对接 ClickHouse 表，支持初始化、一次性更新与常驻自动更新。
- 目标：在目标环境完成配置与部署；按测试报告执行用例并记录结果；满足验收项并具备回滚能力。

## 2. 版本信息
- 模块：`cksr`（Go 1.25.1）
- 主要依赖：`spf13/cobra`、`clickhouse-go`、`go-sql-driver/mysql` 等（参见 `go.mod`）。
- 配置示例：仓库根目录 `config.json`。

## 3. 环境与前置条件
- 数据库：可访问的 StarRocks 与 ClickHouse 集群。
- 账户：SR/CH 账号具有视图创建、表重命名与查询权限。
- 网络：允许从运行 `cksr` 的环境访问 SR MySQL 协议与 CH HTTP/Native 端口。
- 运行：可在容器或主机执行二进制；若启用锁（默认非调试），建议在 Kubernetes 集群内运行。

## 4. 配置与部署
- 配置文件：复制并调整根目录 `config.json`，主要关注：
  - `starrocks` 与 `clickhouse` 连接信息与数据库名。
  - `catalog_name` 与 `sr_table_suffix`（控制 SR 表命名策略）。
  - `timestamp_columns`（覆盖或指定分区时间列类型）。
  - `logger`（日志级别、输出到文件或控制台）。
  - `updater` 与 `lock`（常驻更新器与分布式锁）。
- 构建与分发：
  - 使用 `make export` 导出二进制；或在 CI 中交付对应平台产物。
- 验证：执行 `cksr --help` 与子命令 `init/update/rollback/auto-update --help` 检查参数与环境连接。

## 5. 使用与验收项
- 初始化：`cksr init -c config.json`
  - 结果：为每个映射表创建后缀表与视图；视图定义包含 `union all`；`catalog` 存在。
- 一次性更新：`cksr update -c config.json --pair <pair> --table <view> --partition <值>`
  - 规则：分区值必须与时间列类型匹配（`datetime/date` 用引号，`bigint` 用数值）。
  - 结果：视图 `ALTER` 成功，定义包含新分界；可查询。
- 常驻更新器：`cksr auto-update -c config.json`
  - 结果：按策略周期检查与更新分界；需锁避免与一次性更新并发冲突。
- 回滚：`cksr rollback -c config.json`
  - 结果：删除视图与相关变更；将后缀 SR 表重命名回基础名。

验收项对照：
- 视图创建与定义完整（含 `union all`）。
- 有数据场景更新后可查询且行数 ≥ 1（详见 01、02）。
- 参数错误与类型不匹配均能给出明确错误（详见 08、09、10）。
- 回滚具备预检与冲突提示（详见 07），正常路径可清理对象（详见 04）。
- 部分状态收敛能力（新增）：
  - 视图部分存在或缺失下的初始化（详见 11、12、18）。
  - 视图已删或表已去后缀下的回滚（详见 14、15、16、17）。
  - 批量场景：所有后缀表已重命名为基础名且无视图，初始化可批量收敛（详见 13）。

## 6. 测试执行
- 参考文档：`docs/test-report.md`
- 执行方式：
  - `make test`（自动导出并运行所有用例）
  - 或 `CKSR_BIN=dist/linux-amd64/cksr bash tests/run_all.sh`
  - 单用例：`make run-case CASE=tests/cases/<script>.sh`
- 记录方式：将测试报告中的“未执行”替换为“通过/失败”，并粘贴关键日志截图或错误信息。

## 7. 故障排查与回滚
- 常见错误：
  - 分区值类型不匹配（为 `datetime` 传未加引号数值；为 `bigint` 传字符串）。
  - SR/CH 连接失败（检查主机、端口与凭证）。
  - 锁冲突（一次性更新与常驻更新并发）。
- 排查建议：提高日志级别至 `DEBUG`；使用 `--log-to-file` 输出到文件；检查 SQL 构造与执行失败点。
- 回滚步骤：
  - 执行 `cksr rollback -c config.json`。

## 8. 交付物清单
- 说明文档：`README.md`
- 测试报告：`docs/test-report.md`
- 转测文档：`docs/transfer-guide.md`
- 示例配置：`config.json`
- 执行脚本：`tests/run_all.sh` 与各用例脚本
- 构建脚本：`Makefile`
