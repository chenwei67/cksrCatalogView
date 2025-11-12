#!/usr/bin/env bash
set -euo pipefail

# 用例：回滚（删除视图、后缀表重命名回基础名、删除CK新增列）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
first_sql=$(ls -1 "${SQL_DIR}"/*.sql 2>/dev/null | head -n1 || true)
if [[ -z "${first_sql}" ]]; then
  echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件，无法派生对象名"; exit 1;
fi
BASE_NAME="$(basename "${first_sql}")"; BASE_NAME="${BASE_NAME%.sql}"

pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"
info "[准备] 初始化视图，确保可回滚的状态"
step "执行 初始化"
cksr init --config ./config.json

step "执行 回滚"
cksr rollback --config ./config.json

info "[断言] 基础名表存在，基础视图不存在"
assert_sr_table_exists "${BASE_NAME}" "回滚后缺少基础名表 ${BASE_NAME}"
assert_sr_view_not_exists "${BASE_NAME}" "回滚后视图 ${BASE_NAME} 仍存在"


info "[通过] 03_rollback"