#!/usr/bin/env bash
set -euo pipefail

# 用例15：datetime 类型更新（错误字符串失败 + 正确字符串成功）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/asserts.sh
source tests/helpers/cleanup.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

step "执行 初始化"
cksr init --config ./config.json

BASE_NAME="datalake_platform_log"

step "执行A datetime错误格式（预期失败）"
outA=$(cksr update --config ./config.json --pair "$PAIR_NAME" --table "${BASE_NAME}" --partition '2025/11/12 00:00:00' 2>&1 || true)
if echo "$outA" | grep -Eiq "构建ALTER VIEW SQL失败|执行ALTER VIEW语句失败"; then
  info "[断言A] 错误信息包含：构建/执行 ALTER VIEW 失败"
else
  echo "预期失败但输出不包含预期错误信息：$outA"; exit 1
fi

step "执行B datetime正确格式（预期成功）"
if cksr update --config ./config.json --pair "$PAIR_NAME" --table "${BASE_NAME}" --partition '2025-11-12 00:00:00'; then
  info "[断言B] 成功符合预期"
  assert_sr_view_contains "${BASE_NAME}" "'2025-11-12 00:00:00'" "视图 ${BASE_NAME} 未包含分区 '2025-11-12 00:00:00'"
  # 视图可查询（不要求有数据）
  assert_sr_view_select_ok "${BASE_NAME}" "视图 ${BASE_NAME} 查询失败"
else
  echo "预期成功但实际失败：datetime 正确格式仍失败"; exit 1;
fi

info "[通过] 09_update_invalid_datetime_string"