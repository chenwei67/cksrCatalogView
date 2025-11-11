#!/bin/bash

# 比较当前目录下 database-sql 子目录的 .sql 文件名与 StarRocks 中（剔除指定后缀后）的表名差异
# 使用方法：
#   ./compare_sql_sr_tables.sh [配置文件路径(默认./config.json)] [SQL目录(默认./database-sql)]
# 示例：
#   ./compare_sql_sr_tables.sh /d/Users/User/Desktop/cksr/config.json ./database-sql

set -e

CONFIG_FILE="${1:-./config.json}"
SQL_DIR="${2:-./database-sql}"

# 依赖检查
if ! command -v jq &> /dev/null; then
  echo "错误: 需要安装 jq 来解析 JSON 配置文件";
  exit 1;
fi

if ! command -v mysql &> /dev/null; then
  echo "错误: 需要安装 mysql 客户端来连接 StarRocks";
  exit 1;
fi

# 路径与目录检查
if [ ! -f "$CONFIG_FILE" ]; then
  echo "错误: 配置文件不存在: $CONFIG_FILE";
  exit 1;
fi

if [ ! -d "$SQL_DIR" ]; then
  echo "错误: SQL 目录不存在: $SQL_DIR";
  exit 1;
fi

echo "读取配置文件: $CONFIG_FILE";

# 读取配置
SR_HOST=$(jq -r '.database_pairs[0].starrocks.host' "$CONFIG_FILE")
SR_PORT=$(jq -r '.database_pairs[0].starrocks.port' "$CONFIG_FILE")
SR_USERNAME=$(jq -r '.database_pairs[0].starrocks.username' "$CONFIG_FILE")
SR_PASSWORD=$(jq -r '.database_pairs[0].starrocks.password' "$CONFIG_FILE")
SR_DATABASE=$(jq -r '.database_pairs[0].starrocks.database' "$CONFIG_FILE")
SR_TABLE_SUFFIX=$(jq -r '.database_pairs[0].sr_table_suffix' "$CONFIG_FILE")

# 简单校验
if [ -z "$SR_HOST" ] || [ "$SR_HOST" = "null" ] || \
   [ -z "$SR_PORT" ] || [ "$SR_PORT" = "null" ] || \
   [ -z "$SR_USERNAME" ] || [ "$SR_USERNAME" = "null" ] || \
   [ -z "$SR_DATABASE" ] || [ "$SR_DATABASE" = "null" ]; then
  echo "错误: 配置文件中的 StarRocks 配置信息不完整";
  exit 1;
fi

echo "StarRocks 连接: $SR_HOST:$SR_PORT 数据库: $SR_DATABASE 用户: $SR_USERNAME";
[ -n "$SR_TABLE_SUFFIX" ] && [ "$SR_TABLE_SUFFIX" != "null" ] && echo "过滤后缀: $SR_TABLE_SUFFIX";

# 构造 mysql 参数（通过环境变量传递密码以避免警告）
MYSQL_OPTS="-h$SR_HOST -P$SR_PORT -u$SR_USERNAME"
if [ -n "$SR_PASSWORD" ] && [ "$SR_PASSWORD" != "null" ]; then
  export MYSQL_PWD="$SR_PASSWORD"
fi

# 测试连接
if ! mysql $MYSQL_OPTS -e "SELECT 1;" &> /dev/null; then
  echo "错误: 无法连接到 StarRocks 数据库";
  exit 1;
fi

echo "连接成功，读取所有表名...";
ALL_TABLES=$(mysql $MYSQL_OPTS -D "$SR_DATABASE" -e "SHOW TABLES;" -s -N)

# 读取 SQL 文件名（不含扩展名）
shopt -s nullglob
declare -A FILE_TABLES_SET
for f in "$SQL_DIR"/*.sql; do
  base="$(basename "$f" .sql)"
  lower="${base,,}"
  FILE_TABLES_SET["$lower"]=1
done

FILE_COUNT=${#FILE_TABLES_SET[@]}
echo "SQL 文件数: $FILE_COUNT"

# 过滤后缀，构建 SR 表集合
declare -A SR_TABLES_SET
FILTER_SUFFIX="$SR_TABLE_SUFFIX"

FILTERED_COUNT=0
KEPT_COUNT=0
while IFS= read -r t; do
  [ -z "$t" ] && continue
  if [ -n "$FILTER_SUFFIX" ] && [ "$FILTER_SUFFIX" != "null" ]; then
    case "$t" in
      *"$FILTER_SUFFIX")
        FILTERED_COUNT=$((FILTERED_COUNT+1))
        continue
        ;;
    esac
  fi
  lower_t="${t,,}"
  SR_TABLES_SET["$lower_t"]=1
  KEPT_COUNT=$((KEPT_COUNT+1))
done <<< "$ALL_TABLES"

echo "SR 数据库保留表数(已过滤后缀): $KEPT_COUNT (过滤掉 $FILTERED_COUNT)"

# 计算差异
MISSING_IN_SR=()
for name in "${!FILE_TABLES_SET[@]}"; do
  if [ -z "${SR_TABLES_SET[$name]}" ]; then
    MISSING_IN_SR+=("$name")
  fi
done

MISSING_IN_FILES=()
for name in "${!SR_TABLES_SET[@]}"; do
  if [ -z "${FILE_TABLES_SET[$name]}" ]; then
    MISSING_IN_FILES+=("$name")
  fi
done

# 输出结果
echo "—— 比较结果 ——"
echo "在 SQL 目录中存在但 SR(过滤后)不存在的表: ${#MISSING_IN_SR[@]}"
for n in "${MISSING_IN_SR[@]}"; do echo "  - $n"; done

echo "在 SR(过滤后)存在但 SQL 目录中缺失的表: ${#MISSING_IN_FILES[@]}"
for n in "${MISSING_IN_FILES[@]}"; do echo "  - $n"; done

exit 0