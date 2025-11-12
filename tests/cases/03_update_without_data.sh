#!/usr/bin/env bash
set -euo pipefail

# 用例：一次性更新视图（无数据，使用默认值）
source tests/helpers/config.sh ./config.json
source tests/helpers/asserts.sh
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"
# 准备：初始化视图
step "执行 初始化"
cksr init --config ./config.json
found_any=false
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  found_any=true
  BASE_NAME="$(basename "$f")"; BASE_NAME="${BASE_NAME%.sql}"
  spec="$(detect_timestamp_column_for "${BASE_NAME}")"; col="${spec%%|*}"; typ="${spec##*|}"
  target_table="${BASE_NAME}${SR_SUFFIX}"
  # 清空物理表数据以触发默认值路径
  mysql_exec "TRUNCATE TABLE \`${target_table}\`"

  # 计算默认分区值并执行更新
  RAW_PARTITION_VALUE="$(suggest_partition_for_view "${BASE_NAME}")"
  PARTITION="$(format_partition_for_view "${BASE_NAME}" "${RAW_PARTITION_VALUE}")"
  step "执行 无数据更新 ${BASE_NAME}，默认分区 ${PARTITION}"
  cksr update --config ./config.json --pair "$PAIR_NAME" --table "${BASE_NAME}" --partition "${PARTITION}"

  info "[断言] 视图定义包含默认分区值"
  assert_sr_view_contains "${BASE_NAME}" "${PARTITION}" "视图 ${BASE_NAME} 未包含分区 ${PARTITION}"
done
if [[ "$found_any" != true ]]; then
  echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件"; exit 1;
fi

info "[通过] 03_update_without_data"