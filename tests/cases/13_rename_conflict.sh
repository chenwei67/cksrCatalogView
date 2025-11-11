#!/usr/bin/env bash
set -euo pipefail

# 用例：回滚预检失败（重命名冲突）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/asserts.sh

SQL_DIR="./temp/sqls"
first_sql=$(ls -1 "${SQL_DIR}"/*.sql 2>/dev/null | head -n1 || true)
if [[ -z "${first_sql}" ]]; then
  echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件，无法派生对象名"; exit 1;
fi
BASE_NAME="$(basename "${first_sql}")"; BASE_NAME="${BASE_NAME%.sql}"

warn "[清理前置] 删除可能存在的视图与表，确保干净环境"
sr_drop_view_if_exists "${BASE_NAME}" || true
sr_drop_table_if_exists "${BASE_NAME}${SR_SUFFIX}" || true
sr_drop_table_if_exists "${BASE_NAME}" || true

info "[准备] 人为制造 SR 基础名与后缀名同时存在的冲突，且基础名不是视图"
mysql_exec "CREATE TABLE IF NOT EXISTS \`${BASE_NAME}\` (id INT)"
mysql_exec "CREATE TABLE IF NOT EXISTS \`${BASE_NAME}${SR_SUFFIX}\` (id INT)"

step "执行 回滚（预期失败，提示重命名冲突）"
if cksr rollback --config ./config.json; then
  echo "预期失败但实际成功：未检测到重命名冲突"; exit 1;
else
  info "[断言] 失败符合预期，包含重命名冲突语义"
fi

:

info "[通过] 13_rename_conflict（预期失败用例）"