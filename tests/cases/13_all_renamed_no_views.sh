#!/usr/bin/env bash
set -euo pipefail

# 用例19：所有表都重命名为基础名且没有视图，执行 init 应批量补齐视图并按策略重命名为后缀表
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

step "首次初始化，创建视图与后缀表"
cksr init --config ./config.json

step "模拟批量场景：删除全部视图并将所有后缀表重命名回基础名"
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

step "执行初始化，期望批量补齐视图并恢复后缀命名"
cksr init --config ./config.json

step "断言：所有视图均存在且定义完整，后缀表也存在"
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  assert_sr_view_exists "${base}" "视图未补齐：${base}"
  assert_sr_view_contains "${base}" "union all" "视图 ${base} 定义不包含 union all"
  assert_sr_table_exists "${base}${SR_SUFFIX}" "缺少后缀表：${base}${SR_SUFFIX}"
done

info "[通过] 13_all_renamed_no_views（批量场景：全部重命名且无视图）"