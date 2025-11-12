#!/usr/bin/env bash
set -euo pipefail

# 用例18：仅存在后缀表（视图缺失）情况下初始化应补齐所有视图
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

step "首次初始化，创建视图与后缀表"
cksr init --config ./config.json

step "模拟部分状态：删除所有视图，仅保留后缀表"
found_any=false
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"; found_any=true
  sr_drop_view_if_exists "${base}"
  assert_sr_table_exists "${base}${SR_SUFFIX}" "缺少后缀表 ${base}${SR_SUFFIX}"
done
if [[ "$found_any" != true ]]; then
  echo "错误：${SQL_DIR} 下未找到 .sql 文件"; exit 1;
fi

step "再次执行初始化，期望补齐所有缺失视图"
cksr init --config ./config.json

step "断言：所有视图均存在且定义完整"
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  assert_sr_view_exists "${base}" "视图未补齐：${base}"
  assert_sr_view_contains "${base}" "union all" "视图 ${base} 定义不包含 union all"
done

info "[通过] 12_partial_init_all_existing_views"