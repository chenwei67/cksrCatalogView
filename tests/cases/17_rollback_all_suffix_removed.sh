#!/usr/bin/env bash
set -euo pipefail

# 用例23：删除所有视图并批量去除所有后缀后执行回滚（应稳健收敛）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

step "初始化创建视图与后缀表"
cksr init --config ./config.json

step "批量删除视图并去除所有后缀（重命名为基础名表）"
found_any=false
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"; found_any=true
  sr_drop_view_if_exists "${base}"
  mysql_exec "ALTER TABLE \`${base}${SR_SUFFIX}\` RENAME \`${base}\`"
done
if [[ "$found_any" != true ]]; then
  echo "错误：${SQL_DIR} 下未找到 .sql 文件"; exit 1;
fi

step "执行回滚"
cksr rollback --config ./config.json

step "断言：所有后缀表不存在、基础名表存在、视图不存在"
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  assert_sr_table_not_exists "${base}${SR_SUFFIX}" "后缀表仍存在：${base}${SR_SUFFIX}"
  assert_sr_table_exists "${base}" "基础名表不存在：${base}"
  assert_sr_view_not_exists "${base}" "视图仍存在：${base}"
done

info "[通过] 17_rollback_all_suffix_removed"