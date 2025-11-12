#!/usr/bin/env bash
set -euo pipefail

# 用例：幂等创建（视图已存在时再次 init）
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

step "执行 第一次 init（创建视图与后缀表）"
cksr init --config ./config.json

step "执行 第二次 init（幂等）"
cksr init --config ./config.json

info "[断言] 视图仍存在，且定义有效"
assert_sr_view_exists "${BASE_NAME}" "视图 ${BASE_NAME} 不存在"
assert_sr_view_contains "${BASE_NAME}" "union all" "视图 ${BASE_NAME} 定义不包含 union all"

# 加强断言：验证关键列存在，并确认视图可查询
spec="$(detect_timestamp_column_for "${BASE_NAME}")"; col="${spec%%|*}"; typ="${spec##*|}"
assert_sr_describe_contains "${BASE_NAME}" "${col}" "视图 ${BASE_NAME} 不包含时间列 ${col}"
assert_sr_view_select_ok "${BASE_NAME}" "视图 ${BASE_NAME} 查询失败"


info "[通过] 05_idempotent_init"