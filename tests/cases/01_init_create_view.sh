#!/usr/bin/env bash
set -euo pipefail

# 用例：初始化创建视图（SR基础名是原生表）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

echo "[执行] 初始化创建视图"
step "执行 初始化创建视图"
cksr init --config ./config.json

echo "[断言] 基于 SQL 文件名派生的对象是否存在"
found_any=false
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"
  base="${name%.sql}"
  found_any=true
  assert_sr_table_exists "${base}${SR_SUFFIX}" "缺少后缀表 ${base}${SR_SUFFIX}"
  assert_sr_view_exists "${base}" "缺少视图 ${base}"
  assert_sr_view_contains "${base}" "union all" "视图 ${base} 定义不包含 union all"
done
if [[ "$found_any" != true ]]; then
  echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件，无法派生断言对象名"; exit 1;
fi

echo "[通过] 01_init_create_view"