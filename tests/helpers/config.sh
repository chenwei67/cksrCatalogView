#!/usr/bin/env bash
set -euo pipefail

# 使用： source tests/helpers/config.sh ./config.json
CONFIG_FILE="${1:-./config.json}"

if ! command -v jq >/dev/null 2>&1; then
  echo "错误: 需要安装 jq" >&2; exit 1;
fi
if ! command -v mysql >/dev/null 2>&1; then
  echo "错误: 需要安装 mysql 客户端" >&2; exit 1;
fi

SR_HOST=$(jq -r '.database_pairs[0].starrocks.host' "$CONFIG_FILE")
SR_PORT=$(jq -r '.database_pairs[0].starrocks.port' "$CONFIG_FILE")
SR_USER=$(jq -r '.database_pairs[0].starrocks.username' "$CONFIG_FILE")
SR_PASS=$(jq -r '.database_pairs[0].starrocks.password' "$CONFIG_FILE")
SR_DB=$(jq -r '.database_pairs[0].starrocks.database' "$CONFIG_FILE")
SR_SUFFIX=$(jq -r '.database_pairs[0].sr_table_suffix' "$CONFIG_FILE")
PAIR_NAME=$(jq -r '.database_pairs[0].name' "$CONFIG_FILE")
TEMP_DIR=$(jq -r '.temp_dir' "$CONFIG_FILE")
if [[ -z "$TEMP_DIR" || "$TEMP_DIR" == "null" ]]; then TEMP_DIR="./temp"; fi

MYSQL_OPTS=("-h${SR_HOST}" "-P${SR_PORT}" "-u${SR_USER}")
if [[ -n "${SR_PASS}" && "${SR_PASS}" != "null" ]]; then
  MYSQL_OPTS+=("-p${SR_PASS}")
fi

mysql_exec() {
  mysql "${MYSQL_OPTS[@]}" -D "$SR_DB" -e "$1"
}

mysql_query() {
  mysql "${MYSQL_OPTS[@]}" -D "$SR_DB" -s -N -e "$1"
}

# 取得 temp/sqls 下首个 SQL 文件的基础名（去掉 .sql）
first_sql_base_name() {
  local sql_dir="${1:-${TEMP_DIR}/sqls}"
  local first_sql
  first_sql=$(find "$sql_dir" -maxdepth 1 -type f -name "*.sql" | sort | head -n1 || true)
  if [[ -z "$first_sql" ]]; then
    echo ""; return 1
  fi
  local base
  base="$(basename "$first_sql")"; base="${base%.sql}"
  echo "$base"
}

# 从 config.timestamp_columns 中读取某视图的时间列类型（例如 datetime），返回空表示未配置
timestamp_type_for() {
  local view="$1"
  jq -r --arg v "$view" '.timestamp_columns[$v].type // empty' "$CONFIG_FILE"
}

# 探测视图的时间列及类型：仅读取 config.json 的 timestamp_columns；
# 若未配置则使用默认：recordTimestamp|bigint（UNIX 秒）
detect_timestamp_column_for() {
  local view="$1"
  local t
  t=$(timestamp_type_for "$view")
  if [[ -n "$t" ]]; then
    local col
    col=$(jq -r --arg v "$view" '.timestamp_columns[$v].column' "$CONFIG_FILE")
    echo "$col|$t"; return 0
  fi
  echo "recordTimestamp|bigint"
}

# 根据类型格式化分区值：datetime 类型强制单引号包裹；否则原样返回
format_partition_for_view() {
  local view="$1"; shift
  local raw="$*"
  local t
  t=$(timestamp_type_for "$view")
  if [[ "$t" == "datetime" || "$t" == "date" ]]; then
    # 字符串时间类型需加引号（若已带引号则不重复包裹）
    if [[ "$raw" =~ ^'.*'$ ]]; then
      echo "$raw"
    else
      echo "'$raw'"
    fi
  else
    echo "$raw"
  fi
}


# 计算某日期时间的 epoch 秒（本地时区解析）
epoch_of_datetime() {
  local dt="$1"
  date -d "$dt" +%s
}

# 为某视图建议一个分区值：
# - datetime 类型：当天 00:00:00 的字符串
# - 其他（timestamp/bigint）：当天 00:00:00 的 epoch 秒
suggest_partition_for_view() {
  local view="$1"
  local t
  t=$(timestamp_type_for "$view")
  # 与代码实现保持一致的默认值（空表路径）
  if [[ "$t" == "datetime" ]]; then
    echo "9999-12-31 23:59:59"
  elif [[ "$t" == "date" ]]; then
    echo "9999-12-31"
  else
    # bigint/timestamp 等数值类型使用最大占位值
    echo "9999999999999"
  fi
}

# 查询表是否有数据
sr_has_rows() {
  local view="$1"
  local target="${view}${SR_SUFFIX}"
  local c
  c=$(mysql_query "SELECT COUNT(*) FROM \`${target}\`" || echo 0)
  [[ "${c}" -gt 0 ]]
}

# 基于数据范围自动推断分区：
# - 若有数据：
#   * datetime: 取 MAX(col) 并归一到当天00:00:00
#   * bigint/timestamp: 取 MAX(col) 并向下取整到天粒度（86400的倍数）
# - 若无数据：插入一行测试数据（仅填充时间列），再按上规则返回
infer_partition_by_data() {
  local view="$1"
  local spec; spec=$(detect_timestamp_column_for "$view")
  local col="${spec%%|*}"; local typ="${spec##*|}"
  local target="${view}${SR_SUFFIX}"
  local raw=""
  if sr_has_rows "$view"; then
    if [[ "$typ" == "datetime" || "$typ" == "date" ]]; then
      raw=$(mysql_query "SELECT MIN(\`$col\`) FROM \`${target}\`")
    else
      raw=$(mysql_query "SELECT MIN(\`$col\`) FROM \`${target}\`")
    fi
  fi
  if [[ -z "$raw" ]]; then
    # 无数据时使用与实现一致的默认占位值
    raw="$(suggest_partition_for_view "$view")"
  fi
  echo "$raw|$typ|$col"
}

# 如有需要，按时间列和值清理测试数据
# 保留清理函数占位（当前策略不插入数据，通常无需调用）
sr_cleanup_test_rows() {
  local view="$1" col="$2" raw="$3" typ="$4"
  if [[ "$typ" == "datetime" ]]; then
    mysql_exec "DELETE FROM \`$view\` WHERE \`$col\` = '$raw'"
  else
    mysql_exec "DELETE FROM \`$view\` WHERE \`$col\` = $raw"
  fi
}