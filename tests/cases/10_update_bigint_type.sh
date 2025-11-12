#!/usr/bin/env bash
set -euo pipefail

# 用例16：bigint 类型更新（数值成功 + 字符串失败）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/asserts.sh
source tests/helpers/cleanup.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

step "执行 初始化"
cksr init --config ./config.json

BASE_NAME="dns_log" # 未在 timestamp_columns 中配置，默认 recordTimestamp|bigint

# 成功路径：使用当天 00:00:00 的 epoch 秒
RAW_epoch="$(epoch_of_datetime "$(date +'%Y-%m-%d') 00:00:00")"
PARTITION_NUM="${RAW_epoch}"
step "执行A bigint数值（预期成功）"
if cksr update --config ./config.json --pair "$PAIR_NAME" --table "${BASE_NAME}" --partition ${PARTITION_NUM}; then
  info "[断言A] 成功符合预期"
  assert_sr_view_contains "${BASE_NAME}" "${PARTITION_NUM}" "视图 ${BASE_NAME} 未包含分区 ${PARTITION_NUM}"
else
  echo "预期成功但实际失败：bigint 数值更新失败"; exit 1;
fi

# 失败路径：传入字符串时间
step "执行B bigint字符串（预期失败）"
if cksr update --config ./config.json --pair "$PAIR_NAME" --table "${BASE_NAME}" --partition '2025-11-12 00:00:00'; then
  echo "预期失败但实际成功：未校验 bigint 字符串"; exit 1;
else
  info "[断言B] 失败符合预期，bigint 仅接受数值"
fi

info "[通过] 10_update_bigint_type"