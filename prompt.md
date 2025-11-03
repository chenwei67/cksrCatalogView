新 Session Prompt

- 目标：全面落实严格错误外抛策略。除了“常驻异步协程本身不能退出”这一例外，凡是意料之外的错误或经过必要重试仍失败的错误，都必须报错并向外传递直到退出程序；不允许自作主张忽略或 continue 。
- 仓库路径： d:\Users\User\Desktop\cksr_all\cksr
- 必须遵守的现有约束：
  - 日志级别优先级为 CONFIG > FLAG > DEFAULT(INFO) ，非法值必须报错；配置来源的错误使用 WrapConfigErr 包装；使用 ResolveExitCode 解析退出码。
  - CLI 层使用 SilenceUsage: true 、 SilenceErrors: true ，错误交给统一日志与退出码系统控制。
  - 不考虑向后兼容；禁止静默忽略错误或无条件 continue 。
核心要求

- 错误处理策略
  - 对所有函数：一旦出现未预期错误或重试后仍失败，必须 return error ；禁止在同一层面 continue 或吞掉错误后继续做部分工作。
  - 对“常驻异步协程”：协程本身不退出，但每次循环内遇到错误必须记录错误并将错误返回到循环上层，由循环根据策略决定是否继续或退出；不得在循环内部单点静默忽略。
  - 错误分类：配置相关错误统一用 WrapConfigErr(err) 包装；运行期错误直接返回。
- 重试策略
  - 所有带重试的数据库操作（使用 retry.* 包装的）如果最终返回错误，必须把错误向外抛出，不允许“日志 warn 后 continue”。
- 日志与信息
  - 仅通过 logger 输出结构化日志，不在业务层打印裸 log.Printf （除已存在的配置加载完成提示外）。
  - 错误必须具备明确上下文：包含操作对象（库/表/列/索引）、动作用语和原始错误信息。
改造范围（优先顺序）

- internal/initrun/run.go
  - 审核所有 continue 与错误分支。凡是解析失败、检查失败、生成 SQL 失败、执行失败，立即返回错误，不得跳过单表继续下一个表。
  - 保留“必要重试”封装；重试后失败则返回错误。
- internal/rollbackrun/run.go
  - 按 todo.md 第三项要求：每个共同表回滚失败，立即停止整个回滚过程并报错至外层。
- internal/updaterun/run.go
  - 常驻协程不退出，但每次循环应当：
    - 捕获一次循环内部的错误并记录；
    - 将错误返回给循环控制层以便统一策略处理（例如按策略决定是否继续下一次循环或进行报警/降级）。
  - 禁止在循环内部对关键步骤 continue 跳过。
- database/database.go （重点）
  - 所有查询/扫描的错误，禁止用 continue 跳过（例如当前 ExportClickHouseTables() 、 GetStarRocksTableNames() 、索引/列获取等函数）。
  - 将返回集合的函数按严苛策略处理：发生错误时立即返回 error ，不返回部分结果。
  - 对 sql.ErrNoRows 的特殊约定可保留为业务允许的“非错误”场景（例如检查存在性），其他错误上抛。
- cmd/common.go 与 internal/common/common.go
  - 确保 ParseTableFromString 及其调用方对解析失败直接返回错误；不要在解析失败时继续流程。
  - 只保留一个真实来源实现，避免重复定义导致歧义（推荐保留 internal/common.ParseTableFromString 并移除 cmd/common.go 重复函数）。
- 其他目录（ builder/ 、 fileops/ 、 lock/ 、 retry/ 等）
  - 审核所有错误处理路径，确保未出现静默忽略或不必要的 continue 。
  - retry/ 中的策略应只负责重试与最终错误返回；不承担“容错后继续”的业务决策。
实施步骤

- 全局审计与标记
  - 搜索模式： continue 、 logger.Warn( 、 err != nil { 、 if err == nil { 、 ErrNoRows 。
  - 标记所有“错误后继续”的代码位置与原因，并逐一改造为“错误上抛”。
- 函数签名与返回值调整
  - 必要时将只返回部分结果的函数改为遇错直接 return error ，更新所有调用方的错误处理。
- 分类包装与退出码
  - 配置类错误统一 WrapConfigErr ；其余按运行时错误处理。
  - CLI 执行入口（ cmd/root.go 各子命令入口）维持“错误外抛直到退出”的一致性。
- 构建与最小验证
  - 执行 go build ./... 验证编译通过。
  - 可选：为关键路径添加最小单元测试或集成测试（若无测试框架，至少确保逻辑一致与编译正常）。
- 文档同步
  - 在 todo.md 或 自测.md 简要记录“严格错误外抛”变更要点及影响范围。
可接受标准

- 代码中不再存在对非预期错误的静默忽略或“继续处理下一项”的 continue （重试封装除外）。
- 所有失败在重试后仍失败的路径都 return error 并上抛到 CLI 层退出。
- 常驻协程的每次循环都会将错误返回给上层控制逻辑，协程本身不退出但不吞错。
- 构建通过： go build ./... 。
- 注释与日志清晰表达错误来源与下一步退出行为。
环境与运行

- 操作系统：Windows
- 构建命令： go build ./...
- 入口命令： cksr （子命令 init 、 update 、 rollback ）
- 配置加载： config.json （优先级 CONFIG > FLAG > DEFAULT(INFO) ）
如需示例改造指引：

- 将 database.ExportClickHouseTables() 中的
  - if err := rows.Scan(&tableName); err != nil { continue } 改为 return nil, fmt.Errorf("扫描表名失败: %w", err)
  - 获取创建语句失败时不再 continue ，改为 return nil, fmt.Errorf("获取表 %s 的创建语句失败: %w", tableName, err)
- 对所有集合构造函数（列/索引列表获取）采用同样策略：遇错直接返回错误，不保留部分集合。
请按上述 Prompt 执行，优先从 database/ 和 internal/*run 三个目录的“错误后 continue”处着手，逐步推进到其他模块。
