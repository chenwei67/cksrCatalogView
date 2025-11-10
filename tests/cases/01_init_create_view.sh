#!/usr/bin/env bash
set -euo pipefail

# 用例：初始化创建视图（SR基础名是原生表）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/asserts.sh

SQL_DIR="./temp/sqls"
echo "[清理前置] 删除可能存在的视图与表，确保干净环境"
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  sr_drop_view_if_exists "${base}" || true
  sr_drop_table_if_exists "${base}${SR_SUFFIX}" || true
  sr_drop_table_if_exists "${base}" || true
done
echo "[准备] 执行你提供的 SR 建表 SQL（目录：${SQL_DIR}）"
./execute_sql.sh ./config.json "${SQL_DIR}"

echo "[执行] 初始化创建视图"
cksr_safe init --config ./config.json

if should_assert; then
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
else
  echo "[跳过断言] 上一步 init 失败，跳过视图/后缀表断言"
fi

echo "[清理后置] 回滚并删除表，恢复初始状态"
cksr rollback --config ./config.json || true
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  sr_drop_view_if_exists "${base}" || true
  sr_drop_table_if_exists "${base}${SR_SUFFIX}" || true
  sr_drop_table_if_exists "${base}" || true
done

echo "[通过] 01_init_create_view"
asserts_finalize