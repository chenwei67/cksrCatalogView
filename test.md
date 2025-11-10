# 自动化测试

## 测试策略与方法（最新版）
- 目标：所有测试以“按 SR 数据最小时间自动推断分区”为准，不依赖固定常量或环境变量；空表时才使用默认值。
- 适配多视图：遍历 `temp/sqls/*.sql`，逐个以基础名（去掉 `.sql`）作为对象名进行初始化、更新与断言。
- 分区推断（数据驱动，最小值）：
  - 时间列来源仅限 `config.json.timestamp_columns[<view>]`；若缺失则默认 `recordTimestamp|bigint`（UNIX 秒）。
  - 有数据：
    - `datetime|date`：`SELECT MIN(<col>)`（原值，不做归一）。
    - `bigint|timestamp`：`SELECT MIN(<col>)`（原值，不做归一）。
  - 无数据（默认值，与实现严格一致）：
    - `datetime` → `'9999-12-31 23:59:59'`。
    - `date` → `'9999-12-31'`。
    - `bigint|timestamp` → `9999999999999`。
- 分区值格式：`tests/helpers/config.sh` 的 `format_partition_for_view(<view>, <raw>)` 按类型自动加/不加引号：
  - `datetime` → `'YYYY-MM-DD HH:MM:SS'`（带引号）。
  - `date` → `'YYYY-MM-DD'`（带引号）。
  - 数值类型（`bigint/timestamp`）→ 整数秒（不带引号）。
- 统一执行入口：所有用例通过 `tests/helpers/cksr.sh` 的 `cksr` 包装执行；若设置 `CKSR_BIN` 则优先使用导出二进制，否则回退 `go run .`。
- 断言与健壮性：
  - 视图存在：`SHOW FULL TABLES WHERE Table_type='VIEW'` 精确匹配第一列。
  - 视图定义包含：`SHOW CREATE VIEW <view>`，大小写不敏感匹配关键片段（例如分区值）。
  - 出错即停：命令/断言失败立即退出并打印可读错误。

### 自包含与清理（强制）
- 每个测试脚本在自身内部执行前置清理与后置清理，保证独立运行且不污染环境。
- 前置清理（标准化模板）：
  - 遍历 `temp/sqls/*.sql` 派生 `<base>`；删除 `\`<base>\`` 视图、`\`<base><suffix>\`` 表、`\`<base>\`` 表。
  - 使用辅助函数：`sr_drop_view_if_exists`、`sr_drop_table_if_exists`。
- 后置清理（标准化模板）：
  - 调用 `cksr rollback`（容错）恢复视图/后缀表状态。
  - 再次删除残留的视图与表（基础名与后缀名），确保环境回到初始状态。

### 关键辅助函数（tests/helpers/config.sh）
- `detect_timestamp_column_for(<view>)`：返回 `<column>|<type>`，来源仅为 `config.json.timestamp_columns[<view>]`；若缺失则默认 `recordTimestamp|bigint`。
- `infer_partition_by_data(<view>)`：返回 `<raw>|<type>|<column>`，其中 `<raw>` 为按 SR 后缀表最小值推断的分区原始值；无数据则返回默认值（见上）。
- `sr_cleanup_test_rows(<view>, <column>, <raw>, <type>)`：按列与值清理刚插入的测试数据。
- 其他：`mysql_exec/mysql_query`、`today_midnight/epoch_of_datetime`、`format_partition_for_view`（类型感知格式化）。

### 关键辅助函数（tests/helpers/asserts.sh）
- `sr_drop_view_if_exists(<view>)`：若存在则删除视图。
- `sr_drop_table_if_exists(<table>)`：若存在则删除表（基础名或后缀名）。

### 更新用例 02_update_once（数据驱动版，最小值）
- 遍历 `temp/sqls/*.sql`，对每个 `<view>`：
  1) `info=$(infer_partition_by_data <view>)` → 拆出 `raw/type/column`。
  2) `partition=$(format_partition_for_view <view> <raw>)`。
  3) 执行：`cksr update --pair <pair> --table <view>,<partition>`。
  4) 断言：`sr_show_create_view_contains <view> <partition>`。
- 优势：对 `dns_log/“from_unixtime(recordTimestamp)”` 等视图自动选择整数秒；对 `datalake_platform_log/insertTime` 自动选择 `datetime` 字符串；空表场景下使用默认值，无需插入或清理数据。

### 更新用例 02a_update_with_data（显式准备数据，验证最小值路径）
- 前置：执行 `./execute_sql.sh ./config.json ./temp/sqls` 并 `cksr init` 保证视图与后缀表存在。
- 数据准备：对每个 `<view>`，取 `detect_timestamp_column_for(<view>) → <column>|<type>`，在 `\`<view><suffix>\`` 中插入一行时间更早的记录：
  - `datetime` 插入 `'2000-01-02 00:00:00'`；
  - `bigint` 插入 `epoch_of_datetime('2000-01-02 00:00:00')`。
- 执行更新：按 `infer_partition_by_data(<view>)` 推断分区（MIN 路径），格式化后调用 `cksr update`。
- 断言：`SHOW CREATE VIEW` 包含该分区值；用后通过 `sr_cleanup_test_rows` 清理插入行。

### 更新用例 02b_update_without_data（清空数据，验证默认值路径）
- 前置：执行建表并 `cksr init`。
- 数据清理：对每个 `<view>` 的后缀表 `\`<view><suffix>\`` 执行 `TRUNCATE TABLE`，确保无数据。
- 默认值：`suggest_partition_for_view(<view>)` 返回实现规定的最大占位值（`datetime` → `'9999-12-31 23:59:59'`；`date` → `'9999-12-31'`；`bigint|timestamp` → `9999999999999`），`format_partition_for_view` 负责按类型加引号或保持为数值。
- 执行更新与断言：调用 `cksr update` 并验证视图定义包含默认分区值。

### 数据操作原则
- 允许在测试表中插入与删除数据以满足用例推断与断言，所有插入均使用最小影响策略（仅时间列），并在用例尾部清理。
- 不对生产环境执行此策略；仅在测试库或隔离环境下运行。

### 文档与示例统一
- 所有示例不再硬编码具体视图名或分区常量，统一以 `<view>` 与“数据驱动推断值”示意；如需指定特定视图或分区，可在用例中覆盖推断逻辑（后续可扩展 `TARGET_VIEW` 变量）。

---

## 目标
- 生成覆盖正常与异常场景的测试用例与自动化脚本设计，并在你确认后按文档实现。
- 正常场景：视图初始化创建、一次性更新、回滚。
- 异常场景：视图已存在的幂等验证、字段映射/类型不匹配、重命名冲突、参数缺失/非法值、更新分区类型不匹配。

## 项目功能简述（用于测试设计）
- CLI 根命令：`cksr`，支持子命令：
  - `init`：遍历 `config.json` 的数据库对，必要时将 SR 原生表重命名为带后缀（`sr_table_suffix`），然后为基础名创建视图（`CREATE VIEW IF NOT EXISTS`）。
  - `update`：一次性按数据库对与目标视图列表执行 `ALTER VIEW`，需要显式提供分区时间值（支持数值或字符串）。用法：`cksr update --pair <pair> --table <view>,<partition> ...`。
  - `auto-update`：常驻定时器，按 `config.view_updater.cron_expression` 周期批量更新视图时间边界（持有分布式锁，避免并发冲突）。
  - `rollback`：两阶段回退。删除基础名视图（若存在），将 SR 后缀表重命名回基础名（若允许），并删除 CK 中新增的列（由初始化阶段添加）。
- 通用参数：`--config <path>` 指定配置文件；`--log-level <level>` 设置日志等级（默认 INFO，配置文件优先）。
- 关键验证逻辑：
  - 视图创建/更新基于字段映射严格校验（`builder.ViewBuilder.PrepareAndValidate`）。若 CK/SR 字段不一致或缺失会报错并终止。
  - 回滚前预检重命名冲突：当目标基础名在 SR 中已存在且不是视图时，阻止后缀表重命名，并以“重命名冲突”失败。

## 测试环境准备
- 使用 `./config.json` 配置测试环境（SR/CK 连接、`sr_table_suffix`、`timestamp_columns`、`view_updater.cron_expression` 等）。
- 所有待测试的 SR/CK 表的建表 SQL 文件放在目录 `./temp/sqls/` 下（你将提供或我稍后补充示例）。
- 可使用 `./execute_sql.sh` 执行 SQL（支持传参）：
  - `./execute_sql.sh ./config.json ./temp/sqls` 批量执行 `./temp/sqls` 下的 `.sql`。
- 依赖工具（建议在 Git Bash 或 WSL 环境执行）：
  - `jq`（解析 JSON），`mysql`（连接 StarRocks）。
  - Go 1.21+（仅在未导出二进制时回退使用）：通过测试助手 `cksr` 函数自动选择。若设置环境变量 `CKSR_BIN` 指向导出的二进制，则直接使用二进制；否则回退执行 `go run .`。
  - 使用方式：`source tests/helpers/cksr.sh` 后，运行 `cksr <subcommand> --config ./config.json`。

## 基础测试数据与约定
- 以 `config.json` 中存在的时间戳列配置为基准（示例：`<view>.insertTime` 为 `datetime`）。
- 表命名约定：基础名统一以 CK/SR 共同的真实表名（记作 `<view>`）；SR 后缀名为 `<view><sr_table_suffix>`（如 `_local_catalog`）。
- 断言查询约定（SR）：
  - 检查视图存在：`SHOW FULL TABLES WHERE Table_type='VIEW';` 或 `SHOW CREATE VIEW <db>.<view>;`
  - 检查表存在：`SHOW TABLES LIKE '<name>';`
  - 过滤后缀：参考 `./query_all_tables.sh` 的逻辑。

## 测试用例设计

### 正常场景
- 初始化创建视图（SR 基础名是原生表）：
  - 前置：在 `./temp/sqls/` 准备 CK/SR 同构表（字段可映射）；以目录中首个 `.sql` 文件的基础名作为视图名。
  - 操作：`cksr init --config ./config.json`。
  - 期望：
- SR 原生表被重命名为 `<view><suffix>`。
- 创建基础名视图 `<view>`（`CREATE VIEW IF NOT EXISTS`）。
- 校验 `SHOW CREATE VIEW business.<view>` 包含 `union all` 与时间边界条件。

- 一次性更新视图（带分区）：
  - 前置：上一用例已创建视图；通过 `infer_partition_by_data(<view>)` 或在无数据时用 `suggest_partition_for_view(<view>)` 计算分区边界值（按类型格式化）。
  - 操作：`cksr update --config ./config.json --pair <pair> --table <view>,<partition>`（`<view>` 为 `./temp/sqls` 首个 SQL 文件基础名）。
  - 期望：
    - `ALTER VIEW` 成功，无错误日志。
  - `SHOW CREATE VIEW business.<view>` 中的条件更新为新分区值（`<` 与 `>=` 两段）。

- 回滚：
  - 前置：已有基础名视图与后缀 SR 表。
  - 操作：`cksr rollback --config ./config.json`。
  - 期望：
- 删除 `business.<view>` 视图。
- 将 `<view><suffix>` 重命名回基础名 `<view>`。
    - CK 中由初始化阶段新增的列被删除（若存在）。

### 异常场景
- 幂等创建（视图已存在）：
  - 前置：已完成一次 `init`。
  - 操作：再次运行 `cksr init --config ./config.json`。
  - 期望：
    - 因 `CREATE VIEW IF NOT EXISTS`，无报错；日志显示跳过或成功。

- 字段映射/类型不匹配（导致创建/更新失败）：
  - 原则（SR 优先）：SR 的字段定义是目标规范。初始化阶段会尝试通过在 CK 添加“别名列”来适配 SR 所需字段；若 CK 无法提供所需字段或类型不兼容，则严格校验失败。
  - 前置（实现用例）：在 SR 中创建一个包含额外列的基础表以制造与 CK 不一致的映射（例如 `dns_log` 增加一列）。
  - 操作：`cksr init --config ./config.json`（预期失败）。
  - 期望：
    - `builder.ViewBuilder.PrepareAndValidate` 报错：字段映射不一致或缺失；命令以非零退出码结束。
  - 备选（可选更自包含）：也可在 CK 侧注入最小 DDL（缺少 SR 必需列），同样触发上述失败。

- 重命名冲突（回滚预检失败）：
  - 前置：手工制造冲突场景：同时存在 SR 基础名原生表与后缀表 `<base><suffix>`，且基础名不是视图（`<base>` 取 `./temp/sqls` 首个 SQL 的基础名）。
  - 操作：`cksr rollback --config ./config.json`。
  - 期望：
    - 预检阶段失败，错误信息包含“重命名冲突：目标表 <db>.<table> 已存在且不是视图”。

- 参数缺失/非法（一次性更新）：
  - 操作A：缺少 `--pair` 或 `--table`：`cksr update --config ./config.json`。
  - 期望A：
    - 统一错误包装为配置错误，退出码为 `ExitConfig`（2）；错误信息提示必须提供参数。
  - 操作B：分区类型不匹配（例如为 `datetime` 列传入未加引号的字符串或错误格式）。
  - 期望B：
    - 执行 `ALTER VIEW` 时失败，日志包含执行错误；命令非零退出。

- 自动更新器启动/停止：
  - 操作：`cksr auto-update --config ./config.json`，观察按 `cron_expression` 周期的日志；随后发送中断信号停止。
  - 期望：
    - 成功启动，持锁更新，停止时释放锁并优雅退出。

## 自动化脚本结构

- 目录布局：
  - `tests/helpers/`：测试助手与断言（`config.sh`、`asserts.sh`、`cksr.sh`）。
  - `tests/cases/`：各自包含的测试用例脚本。
  - `tests/fixtures/`：特殊场景的最小 SQL（如映射不一致）。
  - `tests/run_all.sh`：串行执行全部用例、汇总结果与统计。
  
- 助手职责：
  - `config.sh`：解析 `config.json`，导出 SR/CK 连接、后缀、数据库对名称等。
  - `asserts.sh`：表/视图存在性与视图定义片段断言；含 `sr_drop_view_if_exists` 与 `sr_drop_table_if_exists`。
  - `cksr.sh`：统一 `cksr` 命令入口（优先使用导出二进制，其次 `go run .`）。

## 用例矩阵（自包含）

- 01_init_create_view
  - 目的：初始化创建视图与后缀表。
  - 前置：遍历 `temp/sqls` 派生 `<view>`；执行前置清理（删除视图、后缀表、基础表）。
  - 操作：执行表 SQL；运行 `cksr init`。
  - 断言：视图存在；视图定义包含 `union all`；后缀表存在。
  - 后置：`cksr rollback`（容错）后清理视图与表。

- 02a_update_with_data
  - 目的：插入更早时间的数据，验证按最小值推断分区更新视图。
  - 前置：执行前置清理；执行建表与 `cksr init`。
  - 操作：按 `detect_timestamp_column_for` 插入更早记录；`infer_partition_by_data` 推断最小分区；`cksr update`。
  - 断言：`SHOW CREATE VIEW` 包含该分区值；清理插入行。
  - 后置：`cksr rollback` 并清理视图与表。

- 02b_update_without_data
  - 目的：无数据场景下验证默认分区值更新视图。
  - 前置：执行前置清理；执行建表与 `cksr init`；`TRUNCATE` 后缀表。
  - 操作：`suggest_partition_for_view` 获取默认分区；`cksr update`。
  - 断言：视图定义包含默认分区值。
  - 后置：`cksr rollback` 并清理视图与表。

- 03_rollback
  - 目的：回滚删除视图、后缀表重命名回基础名、清除 CK 新增列。
  - 前置：执行前置清理；建表与 `cksr init`。
  - 操作：运行 `cksr rollback`。
  - 断言：基础名表存在；基础视图不存在。
  - 后置：清理基础表。

- 11_idempotent_init
  - 目的：验证再次 `init` 幂等不报错。
  - 前置：执行前置清理；建表与一次 `cksr init`。
  - 操作：再次运行 `cksr init`。
  - 断言：视图仍存在；定义包含 `union all`。
  - 后置：`cksr rollback` 并清理视图与表。

- 12_invalid_mapping（预期失败）
  - 原则：SR 字段为目标规范；若 CK 无法适配（缺失或类型不兼容），严格校验失败。
  - 前置：执行前置清理；在 SR 创建包含额外列的 `dns_log`（与 `temp/sqls` 中基础表名一致）。
  - 操作：运行 `cksr init`（预期失败）。
  - 断言：错误为字段映射不一致/缺失；退出码非零。
  - 后置：`cksr rollback` 并清理异常视图与表。
  - 备注：也可在 CK 侧注入最小 DDL（缺少 SR 必需列）以更彻底自包含。

- 13_rename_conflict（预期失败）
  - 目的：验证回滚预检的重命名冲突。
  - 前置：删除同名对象；手动创建基础名原生表与后缀名表，使基础名不是视图。
  - 操作：运行 `cksr rollback`（预期失败）。
  - 断言：错误信息包含“重命名冲突”。
  - 后置：删除制造的表与同名视图（容错）。

- 14_update_param_errors（预期失败）
  - 目的：验证缺失参数与分区类型不匹配的错误处理。
  - 操作A：缺少 `--pair` 或 `--table` → 预期配置错误退出码。
  - 操作B：为 `datetime` 列传入未加引号数值/非法格式 → 预期 `ALTER VIEW` 执行报错。
  - 后置：统一清理视图与表。
  
- 建议在 Git Bash下执行。
- 先准备表：`./execute_sql.sh ./config.json ./temp/sqls`。
- 逐用例执行或通过 `tests/run_all.sh` 汇总执行。
  - 推荐：先导出二进制并设置环境变量 `CKSR_BIN`，或直接执行 `make test`（Linux amd64）。示例：
    - `make export && export CKSR_BIN="$(pwd)/dist/linux-amd64/cksr" && bash tests/run_all.sh`
    - 或直接：`make test`（自动导出并运行全部用例）。
- 若使用真实数据，请谨慎在生产环境运行回滚测试；优先在隔离的测试库进行。

## 期望输出与判定准则
- 每个用例至少包含：命令返回码、关键日志片段、数据库断言（存在/不存在、视图定义包含关键字）。
- 正常用例全部通过；异常用例需触发预期错误并正确终止（非零退出）。
