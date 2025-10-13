#!/bin/bash

# StarRocks表查询脚本
# 根据config.example.json中的配置连接StarRocks数据库并对所有表执行 select * from table limit 1

set -e  # 遇到错误时退出

# 默认配置文件路径
CONFIG_FILE="${1:-./config.example.json}"

# 检查配置文件是否存在
if [ ! -f "$CONFIG_FILE" ]; then
    echo "错误: 配置文件 $CONFIG_FILE 不存在"
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
SR_TABLE_SUFFIX=$(jq -r '.database_pairs[0].sr_table_suffix' "$CONFIG_FILE")

# 验证配置信息
if [ "$SR_HOST" == "null" ] || [ "$SR_PORT" == "null" ] || [ "$SR_USERNAME" == "null" ] || [ "$SR_DATABASE" == "null" ]; then
    echo "错误: 配置文件中的StarRocks配置信息不完整"
    exit 1
fi

echo "StarRocks连接信息:"
echo "  主机: $SR_HOST"
echo "  端口: $SR_PORT"
echo "  用户名: $SR_USERNAME"
echo "  数据库: $SR_DATABASE"
if [ "$SR_TABLE_SUFFIX" != "null" ] && [ "$SR_TABLE_SUFFIX" != "" ]; then
    echo "  表后缀过滤: $SR_TABLE_SUFFIX"
fi

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

# 获取所有表名
echo "正在获取数据库 $SR_DATABASE 中的所有表..."
ALL_TABLES=$(mysql $MYSQL_OPTS -D "$SR_DATABASE" -e "SHOW TABLES;" -s -N)

if [ -z "$ALL_TABLES" ]; then
    echo "警告: 数据库 $SR_DATABASE 中没有找到任何表"
    exit 0
fi

# 过滤掉带有指定后缀的表
TABLES=""
FILTERED_COUNT=0

if [ "$SR_TABLE_SUFFIX" != "null" ] && [ "$SR_TABLE_SUFFIX" != "" ]; then
    echo "正在过滤带有后缀 '$SR_TABLE_SUFFIX' 的表..."
    while IFS= read -r table; do
        if [[ "$table" == *"$SR_TABLE_SUFFIX" ]]; then
            FILTERED_COUNT=$((FILTERED_COUNT + 1))
            echo "  过滤掉表: $table"
        else
            if [ -z "$TABLES" ]; then
                TABLES="$table"
            else
                TABLES="$TABLES"$'\n'"$table"
            fi
        fi
    done <<< "$ALL_TABLES"
    
    if [ $FILTERED_COUNT -gt 0 ]; then
        echo "已过滤掉 $FILTERED_COUNT 个带有后缀 '$SR_TABLE_SUFFIX' 的表"
    fi
else
    TABLES="$ALL_TABLES"
fi

if [ -z "$TABLES" ]; then
    echo "警告: 过滤后没有剩余的表需要查询"
    exit 0
fi

# 将表名转换为数组
readarray -t TABLE_ARRAY <<< "$TABLES"
TOTAL_TABLES=${#TABLE_ARRAY[@]}

echo "找到 $TOTAL_TABLES 个需要查询的表:"
printf '%s\n' "${TABLE_ARRAY[@]}"
echo ""

# 执行查询统计
SUCCESS_COUNT=0
ERROR_COUNT=0
EMPTY_COUNT=0

echo "开始执行查询..."
echo "========================================"

for table in "${TABLE_ARRAY[@]}"; do
    echo "查询表 $table ..."
    
    # 执行查询并捕获结果
    QUERY="SELECT * FROM \`$table\` LIMIT 1;"
    
    # 使用临时文件存储查询结果和错误信息
    TEMP_OUTPUT=$(mktemp)
    TEMP_ERROR=$(mktemp)
    
    if mysql $MYSQL_OPTS -D "$SR_DATABASE" -e "$QUERY" > "$TEMP_OUTPUT" 2> "$TEMP_ERROR"; then
        # 检查是否有数据返回（除了表头）
        LINE_COUNT=$(wc -l < "$TEMP_OUTPUT")
        if [ "$LINE_COUNT" -gt 1 ]; then
            echo "✓ 成功 (有数据)"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
            
            # 显示查询结果
            echo "查询结果:"
            cat "$TEMP_OUTPUT" | sed 's/^/  /'
        else
            echo "✓ 成功 (空表)"
            EMPTY_COUNT=$((EMPTY_COUNT + 1))
            echo "查询结果: 表为空，无数据"
        fi
    else
        echo "✗ 失败"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        
        # 显示错误信息
        if [ -s "$TEMP_ERROR" ]; then
            echo "  错误信息: $(cat "$TEMP_ERROR")"
        fi
    fi
    
    echo "----------------------------------------"
    
    # 清理临时文件
    rm -f "$TEMP_OUTPUT" "$TEMP_ERROR"
done

echo ""
echo "========================================"
echo "查询完成统计:"
if [ "$SR_TABLE_SUFFIX" != "null" ] && [ "$SR_TABLE_SUFFIX" != "" ] && [ $FILTERED_COUNT -gt 0 ]; then
    echo "  过滤掉的表数: $FILTERED_COUNT"
fi
echo "  查询的表数: $TOTAL_TABLES"
echo "  成功查询(有数据): $SUCCESS_COUNT"
echo "  成功查询(空表): $EMPTY_COUNT"
echo "  查询失败: $ERROR_COUNT"

if [ $ERROR_COUNT -eq 0 ]; then
    echo "✓ 所有表查询完成，无错误"
    exit 0
else
    echo "⚠ 有 $ERROR_COUNT 个表查询失败"
    exit 1
fi