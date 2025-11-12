#!/usr/bin/env bash
set -euo pipefail

# 用例18：删除一个视图并将其后缀表重命名为基础名后，再次 init 应补齐视图并恢复后缀命名
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
  echo "错误：${SQL_DIR} 下未找到 .sql 文件"; exit 1;
fi

step "模拟部分状态：删除一个视图并将对应后缀表重命名回基础名"
sr_drop_view_if_exists "${base}"
mysql_exec "ALTER TABLE \`${base}${SR_SUFFIX}\` RENAME \`${base}\`"

step "再次执行初始化，期望补齐缺失视图并恢复后缀命名"
cksr init --config ./config.json

step "断言：视图已补齐且定义完整，后缀表也已恢复"
assert_sr_view_exists "${base}" "视图未补齐：${base}"
assert_sr_view_contains "${base}" "union all" "视图 ${base} 定义不包含 union all"
assert_sr_table_exists "${base}${SR_SUFFIX}" "缺少后缀表：${base}${SR_SUFFIX}"
assert_sr_view_select_ok "${base}" "视图 ${base} 查询失败"

info "[通过] 18_partial_init_one_suffix_removed"