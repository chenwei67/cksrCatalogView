#!/usr/bin/env bash
set -euo pipefail

# 用例：回滚（删除视图、后缀表重命名回基础名、删除CK新增列）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
first_sql=$(ls -1 "${SQL_DIR}"/*.sql 2>/dev/null | head -n1 || true)
if [[ -z "${first_sql}" ]]; then
  echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件，无法派生对象名"; exit 1;
fi
BASE_NAME="$(basename "${first_sql}")"; BASE_NAME="${BASE_NAME%.sql}"

warn "[清理前置] 删除可能存在的视图与表，确保干净环境"
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  sr_drop_view_if_exists "${base}" || true
  sr_drop_table_if_exists "${base}${SR_SUFFIX}" || true
  sr_drop_table_if_exists "${base}" || true
done

info "[准备] 执行建表并初始化视图，确保可回滚的状态"
./execute_sql.sh ./config.json "${SQL_DIR}"
step "执行 初始化"
cksr init --config ./config.json

step "执行 回滚"
cksr rollback --config ./config.json || true

info "[断言] 基础名表存在，基础视图不存在"
assert_sr_table_exists "${BASE_NAME}" "回滚后缺少基础名表 ${BASE_NAME}"
assert_sr_view_not_exists "${BASE_NAME}" "回滚后视图 ${BASE_NAME} 仍存在"

:

info "[通过] 03_rollback"
asserts_finalize