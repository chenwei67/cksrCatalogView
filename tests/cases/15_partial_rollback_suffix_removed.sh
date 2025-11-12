#!/usr/bin/env bash
set -euo pipefail

# 用例21：部分表已去掉后缀（视图已删除）情况下回滚（应稳健收敛）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

step "初始化创建视图与后缀表"
cksr init --config ./config.json

base="$(first_sql_base_name "${SQL_DIR}")"
if [[ -z "${base}" ]]; then
  echo "错误：${SQL_DIR} 下未找到 .sql 文件"; exit 1;
fi

step "模拟部分状态：删除视图并手动将后缀表重命名回基础名"
sr_drop_view_if_exists "${base}"
# StarRocks 重命名表使用 ALTER TABLE ... RENAME 语法
mysql_exec "ALTER TABLE \`${base}${SR_SUFFIX}\` RENAME \`${base}\`"

step "执行回滚"
cksr rollback --config ./config.json

step "断言：后缀表不存在、基础名表存在、视图不存在"
assert_sr_table_not_exists "${base}${SR_SUFFIX}" "后缀表仍存在：${base}${SR_SUFFIX}"
assert_sr_table_exists "${base}" "基础名表不存在：${base}"
assert_sr_view_not_exists "${base}" "视图仍存在：${base}"

info "[通过] 15_partial_rollback_suffix_removed"