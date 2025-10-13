#!/bin/bash

# StarRocks SQL执行脚本
# 根据config.example.json中的配置连接StarRocks数据库并执行SQL文件

set -e  # 遇到错误时退出

# 默认配置文件路径
CONFIG_FILE="${1:-./config.example.json}"
SQL_DIR="${2:-./database-sql}"

# 检查配置文件是否存在
if [ ! -f "$CONFIG_FILE" ]; then
    echo "错误: 配置文件 $CONFIG_FILE 不存在"
    exit 1
fi

# 检查SQL目录是否存在
if [ ! -d "$SQL_DIR" ]; then
    echo "错误: SQL目录 $SQL_DIR 不存在"
    exit 1
fi

# 检查是否安装了jq工具用于解析JSON
if ! command -v jq &> /dev/null; then
    echo "错误: 需要安装jq工具来解析JSON配置文件"
    echo "请运行: sudo apt-get install jq (Ubuntu/Debian) 或 brew install jq (macOS)"
    exit 1
fi

# 检查是否安装了mysql客户端
if ! command -v mysql &> /dev/null; then
    echo "错误: 需要安装mysql客户端来连接StarRocks"
    echo "请运行: sudo apt-get install mysql-client (Ubuntu/Debian)"
    exit 1
fi

echo "开始解析配置文件: $CONFIG_FILE"

# 从配置文件中提取StarRocks连接信息
SR_HOST=$(jq -r '.database_pairs[0].starrocks.host' "$CONFIG_FILE")
SR_PORT=$(jq -r '.database_pairs[0].starrocks.port' "$CONFIG_FILE")
SR_USERNAME=$(jq -r '.database_pairs[0].starrocks.username' "$CONFIG_FILE")
SR_PASSWORD=$(jq -r '.database_pairs[0].starrocks.password' "$CONFIG_FILE")
SR_DATABASE=$(jq -r '.database_pairs[0].starrocks.database' "$CONFIG_FILE")

# 验证配置信息
if [ "$SR_HOST" = "null" ] || [ "$SR_PORT" = "null" ] || [ "$SR_USERNAME" = "null" ] || [ "$SR_DATABASE" = "null" ]; then
    echo "错误: 配置文件中的StarRocks配置信息不完整"
    exit 1
fi

echo "StarRocks连接信息:"
echo "  主机: $SR_HOST"
echo "  端口: $SR_PORT"
echo "  用户名: $SR_USERNAME"
echo "  数据库: $SR_DATABASE"

# 构建mysql连接参数
MYSQL_OPTS="-h$SR_HOST -P$SR_PORT -u$SR_USERNAME"
if [ "$SR_PASSWORD" != "null" ] && [ "$SR_PASSWORD" != "" ]; then
    MYSQL_OPTS="$MYSQL_OPTS -p$SR_PASSWORD"
fi

# 测试数据库连接
echo "测试数据库连接..."
if ! mysql $MYSQL_OPTS -e "SELECT 1;" &> /dev/null; then
    echo "错误: 无法连接到StarRocks数据库"
    exit 1
fi
echo "数据库连接成功!"

# 查找所有SQL文件
SQL_FILES=$(find "$SQL_DIR" -name "*.sql" -type f | sort)

if [ -z "$SQL_FILES" ]; then
    echo "警告: 在目录 $SQL_DIR 中没有找到SQL文件"
    exit 0
fi

echo "找到以下SQL文件:"
echo "$SQL_FILES"
echo ""

# 执行每个SQL文件
SUCCESS_COUNT=0
TOTAL_COUNT=0

for sql_file in $SQL_FILES; do
    TOTAL_COUNT=$((TOTAL_COUNT + 1))
    echo "正在执行: $sql_file"
    
    # 读取SQL文件内容并替换数据库名
    temp_sql=$(mktemp)
    
    # 替换SQL中的数据库名
    # 这里假设原SQL中使用的是 business.table_name 格式
    # 将其替换为配置文件中指定的数据库名
    sed "s/business\./$SR_DATABASE\./g" "$sql_file" > "$temp_sql"
    
    # 执行SQL
    if mysql $MYSQL_OPTS "$SR_DATABASE" < "$temp_sql"; then
        echo "✓ 成功执行: $sql_file"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "✗ 执行失败: $sql_file"
    fi
    
    # 清理临时文件
    rm -f "$temp_sql"
    echo ""
done

echo "执行完成!"
echo "总计: $TOTAL_COUNT 个文件"
echo "成功: $SUCCESS_COUNT 个文件"
echo "失败: $((TOTAL_COUNT - SUCCESS_COUNT)) 个文件"

if [ $SUCCESS_COUNT -eq $TOTAL_COUNT ]; then
    echo "所有SQL文件执行成功!"
    exit 0
else
    echo "部分SQL文件执行失败，请检查错误信息"
    exit 1
fi