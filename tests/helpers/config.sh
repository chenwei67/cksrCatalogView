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

# 通过环境变量传递密码，避免 mysql 关于命令行密码的警告噪音
_mysql_invoke() {
  if [[ -n "${SR_PASS}" && "${SR_PASS}" != "null" ]]; then
    MYSQL_PWD="${SR_PASS}" mysql "${MYSQL_OPTS[@]}" -D "$SR_DB" "$@"
  else
    mysql "${MYSQL_OPTS[@]}" -D "$SR_DB" "$@"
  fi
}

mysql_exec() {
  _mysql_invoke -e "$1"
}

mysql_query() {
  _mysql_invoke -s -N -e "$1"
}

# 为非空列填充占位默认值，尽量避免插入失败
_sr_placeholder_for_type() {
  local dtype="${1,,}"
  case "$dtype" in
    tinyint|smallint|int|integer|bigint|largeint) echo "0" ;;
    float|double|real) echo "0" ;;
    decimal|numeric) echo "0" ;;
    boolean|bool) echo "0" ;;
    varchar|char|string) echo "''" ;;
    json) echo "'{}'" ;;
    date) echo "'1970-01-01'" ;;
    datetime|timestamp) echo "'1970-01-01 00:00:00'" ;;
    *) echo "0" ;;
  esac
}

# 插入一行包含最小时间的记录，自动为非空列填充值
# 用法： sr_insert_min_timestamp_row <table> <ts_col> <ts_typ> <raw_value>
sr_insert_min_timestamp_row() {
  local table="$1" ts_col="$2" ts_typ="$3" raw="$4" mode="${5:-${SR_INSERT_MODE:-all}}"
  # 读取列定义
  local rows
  rows=$(mysql_query "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='${SR_DB}' AND TABLE_NAME='${table}' ORDER BY ORDINAL_POSITION;")
  local -a cols; local -a vals
  if [[ -z "$rows" ]]; then
    echo "错误：无法读取表结构 ${table}" >&2; return 1
  fi
  while IFS=$'\t' read -r cname isnull dtype cdef; do
    # 选择列集合：all 模式填所有列；required_only 只填必填列（NOT NULL 且无 DEFAULT），时间列始终包含
    if [[ "$mode" == "required_only" && "$cname" != "$ts_col" ]]; then
      if [[ -n "$cdef" && "$cdef" != "NULL" ]]; then
        # 有默认值：省略该列
        continue
      elif [[ "$isnull" == "YES" ]]; then
        # 可空：省略该列
        continue
      fi
      # 否则：必填列，需填占位
    fi

    cols+=("$cname")
    local v=""
    if [[ "$cname" == "$ts_col" ]]; then
      if [[ "$ts_typ" == "datetime" || "$ts_typ" == "date" ]]; then
        v="'${raw}'"
      else
        v="${raw}"
      fi
    else
      if [[ -n "$cdef" && "$cdef" != "NULL" ]]; then
        v="DEFAULT"
      elif [[ "$isnull" == "YES" ]]; then
        v="NULL"
      else
        v="$(_sr_placeholder_for_type "$dtype")"
      fi
    fi
    vals+=("$v")
  done <<< "$rows"

  # 组装并执行 INSERT
  local i cols_sql="" vals_sql=""
  for ((i=0; i<${#cols[@]}; i++)); do
    if [[ $i -gt 0 ]]; then cols_sql+=", "; vals_sql+=", "; fi
    cols_sql+="\`${cols[$i]}\`"
    vals_sql+="${vals[$i]}"
  done
  mysql_exec "INSERT INTO \`${table}\` (${cols_sql}) VALUES (${vals_sql})"
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

# 为 SR 表创建当天零点所在的日分区（上界为次日零点）
# 用法： sr_ensure_today_partition <table> <ts_typ>
# - ts_typ: datetime | date | bigint/timestamp
sr_partition_exists() {
  local table="$1" part="$2"
  local ddl rows
  # 优先解析 SHOW CREATE TABLE 的 DDL，精确匹配分区名
  ddl=$(mysql_query "SHOW CREATE TABLE \`${SR_DB}\`.\`${table}\`" || true)
  if echo "$ddl" | grep -E "PARTITION[[:space:]]+\`$part\`" >/dev/null; then
    return 0
  fi
  # 回退：直接在 SHOW PARTITIONS 输出中查找分区名（不依赖列序）
  rows=$(mysql_query "SHOW PARTITIONS FROM \`${SR_DB}\`.\`${table}\`" || true)
  if echo "$rows" | grep -Fq "$part"; then
    return 0
  fi
  return 1
}

# 检查是否为动态分区表
sr_is_dynamic_partitioned() {
  local table="$1"
  local ddl
  ddl=$(mysql_query "SHOW CREATE TABLE \`${SR_DB}\`.\`${table}\`" || true)
  # 兼容大小写和单双引号
  if echo "$ddl" | grep -E '"dynamic_partition.enable"\s*=\s*"?true"?' >/dev/null; then
    return 0
  fi
  if echo "$ddl" | grep -Ei "DYNAMIC_PARTITION" >/dev/null; then
    # 某些版本以注释块或不同序列化方式呈现，包含 DYNAMIC_PARTITION 即认为开启
    return 0
  fi
  return 1
}

sr_ensure_today_partition() {
  local table="$1" ts_typ="$2"
  local part_name; part_name="p$(date +'%Y%m%d')"
  # 动态分区表不允许手动 ADD/DROP；跳过并提示
  if sr_is_dynamic_partitioned "$table"; then
    echo "提示：\`${table}\` 为动态分区表，跳过手动添加分区 \`${part_name}\`。请通过调整 dynamic_partition.start/end/create_history_partition 覆盖所需日期。" >&2
    return 0
  fi
  # 已存在则直接跳过，避免重复创建报错
  if sr_partition_exists "$table" "$part_name"; then
    return 0
  fi
  if [[ "$ts_typ" == "datetime" ]]; then
    local upper="$(date -d 'tomorrow' +'%Y-%m-%d') 00:00:00"
    mysql_exec "ALTER TABLE \`${table}\` ADD PARTITION \`${part_name}\` VALUES LESS THAN ('${upper}')"
  elif [[ "$ts_typ" == "date" ]]; then
    local upper="$(date -d 'tomorrow' +'%Y-%m-%d')"
    mysql_exec "ALTER TABLE \`${table}\` ADD PARTITION \`${part_name}\` VALUES LESS THAN ('${upper}')"
  else
    local upper_dt="$(date -d 'tomorrow' +'%Y-%m-%d') 00:00:00"
    local upper_epoch; upper_epoch=$(epoch_of_datetime "${upper_dt}")
    mysql_exec "ALTER TABLE \`${table}\` ADD PARTITION \`${part_name}\` VALUES LESS THAN (${upper_epoch})"
  fi
}

export -f sr_ensure_today_partition
export -f sr_partition_exists
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