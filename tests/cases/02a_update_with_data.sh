#!/usr/bin/env bash
set -euo pipefail

# 用例：一次性更新视图（有数据，按最小时间推断）
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
    # 插入一行早期时间数据，确保最小值可被推断
    if [[ "$typ" == "datetime" ]]; then
      raw="2000-01-02 00:00:00"
      mysql_exec "INSERT INTO \`${target_table}\` (\`${col}\`) VALUES ('${raw}')"
    else
      raw="$(epoch_of_datetime "2000-01-02 00:00:00")"
      mysql_exec "INSERT INTO \`${target_table}\` (\`${col}\`) VALUES (${raw})"
    fi

    # 推断分区并执行更新
    info="$(infer_partition_by_data "${BASE_NAME}")" # 将根据最小值返回
    RAW_PARTITION_VALUE="${info%%|*}"; rest="${info#*|}"; typ2="${rest%%|*}"; col2="${rest#*|}"
    PARTITION="$(format_partition_for_view "${BASE_NAME}" "${RAW_PARTITION_VALUE}")"
    echo "[执行] 有数据更新 ${BASE_NAME}，分区 ${PARTITION}（最小值）"
    cksr update --config ./config.json --pair "$PAIR_NAME" --table ${BASE_NAME},${PARTITION}

    echo "[断言] 视图定义包含分区值（最小值）"
    assert_sr_view_contains "${BASE_NAME}" "${PARTITION}" "视图 ${BASE_NAME} 未包含分区 ${PARTITION}"

    # 清理插入的数据
    sr_cleanup_test_rows "${target_table}" "$col" "$RAW_PARTITION_VALUE" "$typ"
  done
  if [[ "$found_any" != true ]]; then
    echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件"; exit 1;
  fi
else
  echo "[跳过断言与更新] 上一步 init 失败，跳过按数据推断分区的更新与断言"
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

echo "[通过] 02a_update_with_data"
asserts_finalize