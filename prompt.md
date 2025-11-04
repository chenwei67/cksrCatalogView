# 任务3：回滚逻辑重构执行提示（Prompt）

目标
- 按 todo.md 的任务3重构回滚：先识别“共同表”，再逐表执行回滚；遇任一错误立即停止并向外抛出。

定义与判定
- 共同表：满足以下条件即视为共同表（用于决定是否执行该表的回滚）：
  - StarRocks 中存在目标业务库下的原始表（或已重命名的新表后缀形式）。
  - ClickHouse 中存在对应的业务表（已添加过列的表）或存在该表的视图（按项目约定命名）。
  - 视图与表可不存在其一；最终以是否满足“需要回滚的任一侧改动”作为是否纳入共同表的依据。

回滚策略（逐表）
- 步序严格：
  1) 删除 ClickHouse 视图（若存在）。
  2) 还原 StarRocks 表名（若已重命名）。
  3) 删除 ClickHouse 中新加的列（若存在）。
- 任一步骤失败：立即停止整个回滚流程并向外抛错（不中断链路、不中途忽略）。

中间态兼容（必须覆盖）
- 视图未创建但 SR 表已重命名：跳过删除视图，执行 SR 表名还原。
- SR 表未改动但 CK 已添加列：跳过 SR 还原，执行 CK 删除新增列。
- Catalog 未创建：与视图相关的操作可能失败；视图缺失应视为业务性跳过（Warn），但任何真实的执行错误需抛出。

错误处理与日志（遵循 todo.md 的“要求”）
- 总则：除业务性跳过外，所有错误必须向外抛出，直到退出程序。
- 重试：数据库查询/执行统一走 `retry` 包；重试耗尽仍失败则上抛，不允许“warn 后继续”。
- 遍历：凡使用 `rows.Next` 遍历，循环后必须检查 `rows.Err`；如有错误立即返回。
- 批量：批量执行遇任一错误立刻返回。
- 常驻协程：不在本任务范围内；本次回滚仍需满足“每次循环内部失败必须上抛并记录”。
- 业务性跳过：仅用于对象缺失导致的跳过（如视图不存在、SR 未改名且无需回滚等），记录 Warn；其余一律错误。
- 日志规范：错误日志必须包含库/表/列/操作与原始错误；Warn 仅用于业务跳过/非致命信息。

实现建议与文件定位
- `builder/rollbackBuilder.go`：
  - 提供逐表的回滚编排方法（删除 CK 视图 → 还原 SR 表名 → 删除 CK 新增列）。
  - 暴露方法示例：`RollbackOneTable(dm *database.DatabasePairManager, table string) error`。
- `internal/rollbackrun/run.go`：
  - 入口层负责：
    - 枚举待处理的表集合（业务库下全部表）；
    - 识别“共同表”（见判定规则）；
    - 按共同表集合逐表调用 `RollbackOneTable`；
    - 任一失败立刻返回错误并终止流程。
- `database/database.go`：
  - 如需补充通用方法：
    - `CheckClickHouseTableExists(table string) (bool, error)`（查 `system.tables`）。
    - `CheckClickHouseViewExists(view string) (bool, error)` 或按约定名派生视图名再查。
    - `DropClickHouseViewIfExists(view string) error`（存在则删，不存在业务性跳过）。
    - `RenameStarRocksTableIfExists(oldName, newName string) error`（存在且已改名则还原，不存在业务性跳过）。
    - `RemoveClickHouseAddedColumnsIfExists(table string, cols []string) error`（新增列来源可由构造器或约定派生）。

命名与约定
- 视图命名约定与列新增约定，以现有 `builder` 的生成策略为准；若未明示，可在 `builder` 中集中管理并复用。
- StarRocks 重命名后缀使用 `DatabasePair.SRTableSuffix`（已有字段）；回滚时按该后缀推导原始名。

依赖与配置
- 使用 `DatabasePairManager` 获取 CK/SR 连接；各连接与超时参数走已配置化的结构。
- 分布式 DDL 超时已配置在 CK 侧（`ClickHouseConfig.distributed_ddl_task_timeout_seconds`）。
- 不做向后兼容处理。

验收标准
- 能在部分对象缺失的“中间态”下正确完成应有的回滚步骤，且遇到真实错误立即停止并上抛。
- 日志与错误信息完整，包含库/表/列/操作与原始错误。
- 所有数据库操作均通过 `retry` 包；遍历均检查 `rows.Err`。
- 本次改动编译通过，现有命令可触发回滚流程并在失败时退出进程。

输出与交付
- 代码改动集中在 `builder/rollbackBuilder.go` 与 `internal/rollbackrun/run.go`，必要的通用方法放在 `database/database.go`。
- 提供最小可运行的回滚流程；后续任务4将引入分布式锁即可在入口层进行串行保护。