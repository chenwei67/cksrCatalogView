#!/usr/bin/env bash
set -euo pipefail

# 用例：一次性更新视图（无数据，使用默认值）
source tests/helpers/config.sh ./config.json
source tests/helpers/asserts.sh
source tests/helpers/cksr.sh

SQL_DIR="${TEMP_DIR}/sqls"
echo "[清理前置] 删除可能存在的视图与表，确保干净环境"
for f in "${SQL_DIR}"/*.sql; do
  [[ -e "$f" ]] || continue
  name="$(basename "$f")"; base="${name%.sql}"
  sr_drop_view_if_exists "${base}" || true
  sr_drop_table_if_exists "${base}${SR_SUFFIX}" || true
  sr_drop_table_if_exists "${base}" || true
done

# 准备：执行建表并初始化视图
./execute_sql.sh ./config.json "${SQL_DIR}"
cksr_safe init --config ./config.json

if should_assert; then
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
    echo "[执行] 无数据更新 ${BASE_NAME}，默认分区 ${PARTITION}"
    cksr update --config ./config.json --pair "$PAIR_NAME" --table ${BASE_NAME},${PARTITION}

    echo "[断言] 视图定义包含默认分区值"
    assert_sr_view_contains "${BASE_NAME}" "${PARTITION}" "视图 ${BASE_NAME} 未包含分区 ${PARTITION}"
  done
  if [[ "$found_any" != true ]]; then
    echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件"; exit 1;
  fi
else
  echo "[跳过断言与更新] 上一步 init 失败，跳过默认分区更新与断言"
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

echo "[通过] 02b_update_without_data"
asserts_finalize