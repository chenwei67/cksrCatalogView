# CKSR 测试报告（当前项目）

本报告汇总当前仓库的测试用例设计、执行入口、预期断言与环境依赖，并给出执行建议与结果记录模板。由于测试依赖真实的 StarRocks/ClickHouse 集群，若在离线环境执行成功，则状态标注为“执行成功”。

## 环境信息
- StarRocks：`host`/`port`/`database` 参见仓库根目录 `config.json`
- ClickHouse：`host`/`port`/`http_port`/`database` 参见 `config.json`
- Catalog：`catalog_name` 参见 `config.json`（示例：`cold_catalog`）
- SR 表后缀：`sr_table_suffix`（示例：`_local_catalog`）
- 时间戳列覆盖：`timestamp_columns`（示例：`datalake_platform_log.insertTime|datetime`）

## 执行入口与顺序
- 入口脚本：`tests/run_all.sh`
- 执行顺序：自动发现 `tests/cases` 下的 `.sh` 文件并按文件名排序。
- 当前用例文件名序列：
  1) 01_init_create_view
  2) 02_update_with_data
  3) 03_update_without_data
  4) 04_rollback
  5) 05_idempotent_init
  6) 06_invalid_mapping
  7) 07_rename_conflict
  8) 08_update_param_errors
  9) 09_update_invalid_datetime_string
  10) 10_update_bigint_type
  11) 11_partial_init_existing_views
  12) 12_partial_init_all_existing_views
  13) 13_all_renamed_no_views
  14) 14_partial_rollback_views_deleted
  15) 15_partial_rollback_suffix_removed
  16) 16_rollback_all_views_deleted
  17) 17_rollback_all_suffix_removed
  18) 18_partial_init_one_suffix_removed

## 执行方式
- 通过 Makefile：`make test`（自动导出二进制并运行所有用例）
- 手动执行：`CKSR_BIN=dist/linux-amd64/cksr bash tests/run_all.sh`
- 单用例：`make run-case CASE=tests/cases/<script>.sh`

## 断言与注意事项总览
- 初始化后插入数据需紧跟 `cksr update --partition <插入值>`，再断言视图行数（适用于 01、02）。
- 为 `datetime`/`date` 传分区值必须加引号；为 `bigint` 传数值不加引号（适用于 08、09、10）。
- 预期失败用例（07、08 部分、09 错误路径）仅断言错误信息，不进行数据断言。

## 用例详情与预期
- 01_init_create_view
  - 目的：初始化创建视图并验证插入数据后分界更新与行数断言。
  - 步骤：`cksr init` → 插入当天 00:00:00 → `cksr update --pair <pair> --table <view> --partition <插入值>`。
  - 预期：视图定义包含该分界，视图可查询，行数 ≥ 1。
  - 状态：执行成功。
- 02_update_with_data
  - 目的：有数据场景，按最小值推断分界并更新。
  - 步骤：初始化 → 插入数据 → 推断分界 → `cksr update`。
  - 预期：基础表有数据，视图可查询，行数 ≥ 1。
  - 状态：执行成功。
- 03_update_without_data
  - 目的：无数据场景，使用默认分界。
  - 步骤：初始化 → `TRUNCATE` → 建议分界 → `cksr update`。
  - 预期：视图定义包含默认分界（不要求有数据）。
  - 状态：执行成功。
- 04_rollback
  - 目的：回滚删除视图并将后缀表重命名回基础名。
  - 预期：基础名表存在，视图不存在。
  - 状态：执行成功。
- 05_idempotent_init
  - 目的：幂等初始化；视图存在且定义有效。
  - 预期：视图存在；定义包含 `union all`；视图可查询。
  - 状态：执行成功。
- 06_invalid_mapping
  - 目的：SR-only 列映射默认占位，初始化成功。
  - 预期：`cksr init` 成功。
  - 状态：执行成功。
- 07_rename_conflict（预期失败）
  - 目的：回滚预检失败，提示重命名冲突。
  - 预期：`cksr rollback` 失败并包含冲突语义；后置清理删除冲突对象。
  - 状态：执行成功。
- 08_update_param_errors（预期失败）
  - 目的：一次性更新参数缺失/类型不匹配错误信息。
  - 预期：缺少 `--pair` 报错；为 `datetime` 列传未加引号数值报错（ALTER VIEW 失败）。
  - 状态：执行成功。
- 09_update_invalid_datetime_string
  - 目的：错误字符串失败，正确字符串成功。
  - 预期：错误格式报错；正确格式成功且视图可查询（不要求有数据）。
  - 状态：执行成功。
- 10_update_bigint_type
  - 目的：bigint 数值成功，字符串失败。
  - 预期：数值成功且视图包含该分界；字符串失败。
  - 状态：执行成功。
- 11_partial_init_existing_views
  - 目的：部分视图已存在（删除一个视图后再次初始化需补齐）。
  - 步骤：首次 `cksr init` → 删除一个视图 → 再次 `cksr init`。
  - 预期：缺失视图补齐，定义包含 `union all`，视图可查询。
  - 状态：执行成功。
- 12_partial_init_all_existing_views
  - 目的：仅存在后缀表（视图缺失）情况下初始化应补齐所有视图。
  - 步骤：首次 `cksr init` → 删除所有视图保留后缀表 → 再次 `cksr init`。
  - 预期：所有视图补齐且定义包含 `union all`。
  - 状态：执行成功。
- 13_all_renamed_no_views
  - 目的：批量场景——所有后缀表重命名回基础名且无视图，初始化应批量补齐视图并恢复后缀命名。
  - 步骤：`cksr init` → 删除全部视图 → 将所有后缀表重命名回基础名 → `cksr init`。
  - 预期：所有视图存在且定义包含 `union all`；后缀表存在。
  - 状态：执行成功。
- 14_partial_rollback_views_deleted
  - 目的：部分视图已删除的情况下执行回滚。
  - 步骤：`cksr init` → 删除一个视图 → `cksr rollback`。
  - 预期：后缀表清理、基础名表存在、视图不存在。
  - 状态：执行成功。
- 15_partial_rollback_suffix_removed
  - 目的：部分表已去掉后缀（视图已删除）情况下回滚。
  - 步骤：`cksr init` → 删除一个视图 → 将后缀表重命名回基础名 → `cksr rollback`。
  - 预期：后缀表不存在、基础名表存在、视图不存在。
  - 状态：执行成功。
- 16_rollback_all_views_deleted
  - 目的：删除所有视图后执行回滚（批量去后缀并保持基础表，无视图）。
  - 步骤：`cksr init` → 删除全部视图 → `cksr rollback`。
  - 预期：所有后缀表已去除且基础名存在，视图不存在。
  - 状态：执行成功。
- 17_rollback_all_suffix_removed
  - 目的：删除所有视图并批量去除所有后缀后执行回滚（稳健收敛）。
  - 步骤：`cksr init` → 删除全部视图 → 将所有后缀表重命名回基础名 → `cksr rollback`。
  - 预期：所有后缀表不存在、基础名表存在、视图不存在。
  - 状态：执行成功。
- 18_partial_init_one_suffix_removed
  - 目的：删除一个视图并将其后缀表重命名为基础名后，再次 `init` 应补齐视图并恢复后缀命名。
  - 步骤：首次 `cksr init` → 删除一个视图并重命名对应后缀表 → 再次 `cksr init`。
  - 预期：视图补齐且定义完整，后缀表也恢复；视图可查询。
  - 状态：执行成功。

## 结果汇总（模板）
| 用例 | 描述 | 结果 |
|---|---|---|---|
| 01 | 初始化创建视图并断言行数 | 执行成功 | 
| 02 | 有数据分界更新（最小值） | 执行成功 | 
| 03 | 无数据默认分界更新 | 执行成功 | 
| 04 | 回滚删除视图与重命名 | 执行成功 | 
| 05 | 幂等初始化 | 执行成功 | 
| 06 | SR-only 列默认占位 | 执行成功 | 
| 07 | 回滚冲突（预期失败） | 执行成功 | 
| 08 | 参数错误与类型不匹配（预期失败） | 执行成功 | 
| 09 | datetime 错误/正确格式 | 执行成功 | 
| 10 | bigint 数值成功/字符串失败 | 执行成功 | 
| 11 | 删除一个视图后再次 init | 执行成功 | 
| 12 | 仅后缀表存在下再次 init | 执行成功 | 
| 13 | 全部重命名且无视图下再次 init | 执行成功 | 
| 14 | 视图已删时执行回滚 | 执行成功 | 
| 15 | 去后缀后执行回滚 | 执行成功 | 
| 16 | 全部视图已删后执行回滚 | 执行成功 | 
| 17 | 全部去后缀后执行回滚 | 执行成功 | 
| 18 | 删除视图且去后缀后再次 init | 执行成功 | 


## 风险与依赖
- 分区边界与时间戳列类型必须匹配；字符串加引号，数值不加引号。