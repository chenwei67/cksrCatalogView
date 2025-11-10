#!/usr/bin/env bash
set -euo pipefail

# 用例：幂等创建（视图已存在时再次 init）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/asserts.sh

SQL_DIR="./temp/sqls"
first_sql=$(ls -1 "${SQL_DIR}"/*.sql 2>/dev/null | head -n1 || true)
if [[ -z "${first_sql}" ]]; then
  echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件，无法派生对象名"; exit 1;
fi
BASE_NAME="$(basename "${first_sql}")"; BASE_NAME="${BASE_NAME%.sql}"

echo "[清理前置] 删除可能存在的视图与表，确保干净环境"
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  sr_drop_view_if_exists "${base}" || true
  sr_drop_table_if_exists "${base}${SR_SUFFIX}" || true
  sr_drop_table_if_exists "${base}" || true
done

echo "[准备] 执行建表以确保自包含"
./execute_sql.sh ./config.json "${SQL_DIR}"

echo "[执行] 第一次 init（创建视图与后缀表）"
cksr init --config ./config.json

echo "[执行] 第二次 init（幂等）"
cksr init --config ./config.json

echo "[断言] 视图仍存在，且定义有效"
sr_view_exists "${BASE_NAME}" || { echo "视图 ${BASE_NAME} 不存在"; exit 1; }
sr_show_create_view_contains "${BASE_NAME}" "union all" || { echo "视图 ${BASE_NAME} 定义不包含 union all"; exit 1; }

echo "[清理后置] 回滚并删除表，恢复初始状态"
cksr rollback --config ./config.json || true
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  sr_drop_view_if_exists "${base}" || true
  sr_drop_table_if_exists "${base}${SR_SUFFIX}" || true
  sr_drop_table_if_exists "${base}" || true
done

echo "[通过] 11_idempotent_init"