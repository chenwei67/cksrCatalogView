#!/usr/bin/env bash
set -euo pipefail

# 用例：初始化创建视图（SR基础名是原生表）
source tests/helpers/config.sh ./config.json
source tests/helpers/cksr.sh
source tests/helpers/cleanup.sh
source tests/helpers/asserts.sh

SQL_DIR="${TEMP_DIR}/sqls"
pre_case_cleanup
ensure_temp_sql_tables "${SQL_DIR}"

echo "[执行] 初始化创建视图"
step "执行 初始化创建视图"
cksr init --config ./config.json

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

  # 加强断言：插入一行测试数据并验证视图可查询且有数据
  spec="$(detect_timestamp_column_for "${base}")"; col="${spec%%|*}"; typ="${spec##*|}"
  target_table="${base}${SR_SUFFIX}"
  step "准备 创建当天分区：${target_table}"
  sr_ensure_today_partition "${target_table}" "${typ}"
  if [[ "$typ" == "datetime" ]]; then
    raw="$(date +'%Y-%m-%d') 00:00:00"
  elif [[ "$typ" == "date" ]]; then
    raw="$(date +'%Y-%m-%d')"
  else
    raw="$(epoch_of_datetime "$(date +'%Y-%m-%d') 00:00:00")"
  fi
  step "准备 插入测试行以校验视图：${base}"
  sr_insert_min_timestamp_row "${target_table}" "${col}" "${typ}" "${raw}" required_only
  # 初始化创建视图时，由于基础表可能为空，视图的时间分界会采用最大占位值(9999...)。
  # 因此需要在插入数据后将视图的分界更新为本次插入的时间值，确保视图可查询到SR侧数据。
  step "更新 视图时间分界到插入值：${base} -> ${raw}"
  if cksr update --config ./config.json --pair "$PAIR_NAME" --table "${base}" --partition "${raw}"; then
    info "[断言] update 成功，视图分界已更新"
    expect_part=$(format_partition_for_view "${base}" "${raw}")
    assert_sr_view_contains "${base}" "${expect_part}" "视图 ${base} 未包含分区 ${expect_part}"
  else
    echo "更新视图分界失败：cksr update --table ${base} --partition ${raw}"; exit 1;
  fi
  assert_sr_base_has_rows "${base}" "基础表 ${target_table} 无数据"
  assert_sr_view_select_ok "${base}" "视图 ${base} 查询失败"
  assert_sr_view_row_count_ge "${base}" 1 "视图 ${base} 行数应至少为 1"
done
if [[ "$found_any" != true ]]; then
  echo "错误：目录 ${SQL_DIR} 下未找到 .sql 文件，无法派生断言对象名"; exit 1;
fi

echo "[通过] 01_init_create_view"