# StarRocks SQL执行脚本使用说明

## 功能描述

`execute_sql.sh` 是一个bash脚本，用于根据配置文件中的StarRocks连接信息，自动执行指定目录下的所有SQL文件。

## 主要功能

1. **自动读取配置**: 从 `config.example.json` 中读取StarRocks数据库连接配置
2. **数据库名替换**: 自动将SQL文件中的数据库名替换为配置文件中指定的数据库名
3. **批量执行**: 扫描指定目录下的所有 `.sql` 文件并依次执行
4. **错误处理**: 提供详细的错误信息和执行结果统计

## 使用方法

### 基本用法
```bash
./execute_sql.sh
```

### 指定配置文件和SQL目录
```bash
./execute_sql.sh [配置文件路径] [SQL文件目录]
```

### 示例
```bash
# 使用默认配置文件和SQL目录
./execute_sql.sh

# 指定配置文件
./execute_sql.sh ./config.json

# 指定配置文件和SQL目录
./execute_sql.sh ./config.json ./my_sql_files
```

## 前置条件

### 必需工具
1. **jq**: JSON解析工具
   ```bash
   # Ubuntu/Debian
   sudo apt-get install jq
   
   # macOS
   brew install jq
   
   # Windows (Git Bash)
   # 下载jq.exe并放入PATH中
   ```

2. **mysql客户端**: 用于连接StarRocks
   ```bash
   # Ubuntu/Debian
   sudo apt-get install mysql-client
   
   # macOS
   brew install mysql-client
   ```

### 目录结构
```
项目根目录/
├── config.example.json    # 配置文件
├── execute_sql.sh         # 执行脚本
└── sql/                   # SQL文件目录（默认）
    ├── table1.sql
    ├── table2.sql
    └── ...
```

## 配置文件格式

脚本会读取配置文件中第一个数据库对的StarRocks配置：

```json
{
  "database_pairs": [
    {
      "starrocks": {
        "host": "10.107.29.99",
        "port": 30938,
        "username": "root",
        "password": "",
        "database": "cw0"
      }
    }
  ]
}
```

## SQL文件处理

### 数据库名替换
脚本会自动将SQL文件中的 `business.` 替换为配置文件中指定的数据库名。

**原SQL文件内容:**
```sql
CREATE TABLE IF NOT EXISTS business.asset_log (
    `uuId` String COMMENT '日志唯一id',
    ...
)
```

**执行时会被替换为:**
```sql
CREATE TABLE IF NOT EXISTS cw0.asset_log (
    `uuId` String COMMENT '日志唯一id',
    ...
)
```

### 支持的SQL语句
- CREATE TABLE 语句
- 其他DDL语句
- 任何包含数据库名的SQL语句

## 执行结果

脚本执行完成后会显示：
- 总文件数
- 成功执行的文件数
- 失败的文件数
- 每个文件的执行状态

## 错误处理

脚本包含以下错误检查：
1. 配置文件存在性检查
2. SQL目录存在性检查
3. 必需工具安装检查
4. 数据库连接测试
5. 每个SQL文件的执行结果检查

## 注意事项

1. **权限**: 确保脚本有执行权限 (`chmod +x execute_sql.sh`)
2. **网络**: 确保能够访问StarRocks数据库服务器
3. **SQL语法**: 确保SQL文件符合StarRocks语法规范
4. **备份**: 执行前建议备份重要数据
5. **测试**: 建议先在测试环境中验证脚本功能

## 故障排除

### 常见问题

1. **jq命令未找到**
   ```
   错误: 需要安装jq工具来解析JSON配置文件
   ```
   解决方案: 安装jq工具

2. **mysql命令未找到**
   ```
   错误: 需要安装mysql客户端来连接StarRocks
   ```
   解决方案: 安装mysql客户端

3. **数据库连接失败**
   ```
   错误: 无法连接到StarRocks数据库
   ```
   解决方案: 检查网络连接和数据库配置信息

4. **配置文件格式错误**
   ```
   错误: 配置文件中的StarRocks配置信息不完整
   ```
   解决方案: 检查配置文件JSON格式和必需字段