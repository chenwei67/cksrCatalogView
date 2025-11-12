#!/usr/bin/env bash
set -euo pipefail

# 用例17：部分视图已存在的情况下再次初始化（缺失视图需补齐）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

step "首次初始化，创建视图与后缀表"
cksr init --config ./config.json

base="$(first_sql_base_name "${SQL_DIR}")"
if [[ -z "${base}" ]]; then
  echo "错误：${SQL_DIR} 下未找到 .sql 文件，无法派生对象名"; exit 1;
fi

step "模拟部分状态：删除一个视图，仅保留其他视图"
sr_drop_view_if_exists "${base}"
assert_sr_table_exists "${base}${SR_SUFFIX}" "缺少后缀表 ${base}${SR_SUFFIX}"

step "再次执行初始化，期望补齐缺失视图"
cksr init --config ./config.json

step "断言：缺失视图已补齐，定义完整且可查询"
assert_sr_view_exists "${base}" "视图未补齐：${base}"
assert_sr_view_contains "${base}" "union all" "视图 ${base} 定义不包含 union all"
assert_sr_view_select_ok "${base}" "视图 ${base} 查询失败"

info "[通过] 11_partial_init_existing_views"