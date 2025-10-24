# CKSR 动态视图更新功能测试用例

## 测试概述

本文档包含了CKSR动态视图更新功能的6种核心测试场景，涵盖了在不同系统状态下进行继续运行和回滚操作的情况。

## 测试环境准备

### 基础配置
```json
{
  "database_pairs": [
    {
      "name": "test-pair",
      "catalog_name": "test_catalog",
      "sr_table_suffix": "_new",
      "clickhouse": {
        "host": "localhost",
        "port": 9000,
        "username": "default",
        "password": "",
        "database": "test_ck"
      },
      "starrocks": {
        "host": "localhost", 
        "port": 9030,
        "username": "root",
        "password": "",
        "database": "test_sr"
      }
    }
  ],
  "view_updater": {
    "enabled": true,
    "cron_expression": "0 */5 * * * *",
    "debug_mode": true,
    "k8s_namespace": "default",
    "lease_name": "cksr-view-updater",
    "identity": "test-instance",
    "lock_duration_seconds": 300
  }
}
```

## 测试场景

### 场景1：CK表已添加字段 + 继续运行

**前置条件：**
- ClickHouse表已存在并添加了新字段（带后缀）
- StarRocks表未重命名（无后缀）
- 视图未创建

**测试步骤：**
1. 启动程序：`./cksr.exe -config config.json`
2. 观察程序行为

**预期结果：**
- ✅ 程序检测到CK表已有新字段，跳过字段添加步骤
- ✅ 程序为SR表添加后缀（重命名）
- ✅ 程序创建联合视图
- ✅ 视图更新器启动成功
- ✅ 定时任务按cron表达式执行视图时间边界更新

**验证命令：**
```sql
-- 检查CK表结构
SHOW CREATE TABLE test_ck.test_table;

-- 检查SR表是否重命名
SHOW TABLES FROM test_sr LIKE '%_new';

-- 检查视图是否创建
SHOW CREATE TABLE test_ck.test_table_view;

-- 检查视图更新器日志
grep "视图更新器启动成功" logs/cksr.log
```

---

### 场景2：CK表已添加字段 + 回滚

**前置条件：**
- ClickHouse表已存在并添加了新字段（带后缀）
- StarRocks表未重命名（无后缀）
- 视图未创建

**测试步骤：**
1. 启动回滚：`./cksr.exe -config config.json -rollback`
2. 观察回滚行为

**预期结果：**
- ✅ 程序获取互斥锁成功
- ✅ 删除所有视图（如果存在）
- ✅ SR表后缀移除（恢复原名）
- ✅ CK表带后缀的列被删除
- ✅ 系统恢复到初始状态

**验证命令：**
```sql
-- 检查CK表字段是否被删除
DESCRIBE test_ck.test_table;

-- 检查SR表是否恢复原名
SHOW TABLES FROM test_sr;

-- 检查视图是否被删除
SELECT count(*) FROM system.tables WHERE database='test_ck' AND engine='View';
```

---

### 场景3：SR表已改名 + 继续运行

**前置条件：**
- ClickHouse表存在但未添加新字段
- StarRocks表已重命名（有后缀）
- 视图未创建

**测试步骤：**
1. 启动程序：`./cksr.exe -config config.json`
2. 观察程序行为

**预期结果：**
- ✅ 程序为CK表添加新字段（带后缀）
- ✅ 程序检测到SR表已重命名，跳过重命名步骤
- ✅ 程序创建联合视图
- ✅ 视图更新器启动成功
- ✅ 定时任务正常执行

**验证命令：**
```sql
-- 检查CK表是否添加了新字段
SHOW CREATE TABLE test_ck.test_table;

-- 检查SR表名称
SHOW TABLES FROM test_sr LIKE '%_new';

-- 检查视图创建
SELECT name FROM system.tables WHERE database='test_ck' AND engine='View';
```

---

### 场景4：SR表已改名 + 回滚

**前置条件：**
- ClickHouse表存在但未添加新字段
- StarRocks表已重命名（有后缀）
- 视图未创建

**测试步骤：**
1. 启动回滚：`./cksr.exe -config config.json -rollback`
2. 观察回滚行为

**预期结果：**
- ✅ 程序获取互斥锁成功
- ✅ 删除所有视图（如果存在）
- ✅ SR表后缀移除（恢复原名）
- ✅ CK表带后缀的列被删除（如果存在）
- ✅ 系统恢复到初始状态

**验证命令：**
```sql
-- 检查SR表是否恢复原名
SHOW TABLES FROM test_sr;

-- 检查CK表字段
DESCRIBE test_ck.test_table;

-- 确认无视图存在
SELECT count(*) FROM system.tables WHERE database='test_ck' AND engine='View';
```

---

### 场景5：视图已创建 + 继续运行

**前置条件：**
- ClickHouse表已添加新字段（带后缀）
- StarRocks表已重命名（有后缀）
- 联合视图已创建

**测试步骤：**
1. 启动程序：`./cksr.exe -config config.json`
2. 观察程序行为

**预期结果：**
- ✅ 程序检测到所有步骤已完成，跳过处理步骤
- ✅ 视图更新器启动成功
- ✅ 定时任务按计划执行视图时间边界更新
- ✅ 视图SQL中的时间戳条件被正确更新

**验证命令：**
```sql
-- 检查视图定义
SHOW CREATE TABLE test_ck.test_table_view;

-- 检查视图更新日志
grep "视图.*更新成功" logs/cksr.log

-- 验证时间边界更新
SELECT min(recordTimestamp) FROM test_sr.test_table_new;
```

---

### 场景6：视图已创建 + 回滚

**前置条件：**
- ClickHouse表已添加新字段（带后缀）
- StarRocks表已重命名（有后缀）
- 联合视图已创建

**测试步骤：**
1. 启动回滚：`./cksr.exe -config config.json -rollback`
2. 观察回滚行为

**预期结果：**
- ✅ 程序获取互斥锁成功（与视图更新器互斥）
- ✅ 所有视图被删除
- ✅ SR表后缀移除（恢复原名）
- ✅ CK表带后缀的列被删除
- ✅ 系统完全恢复到初始状态

**验证命令：**
```sql
-- 确认视图被删除
SELECT count(*) FROM system.tables WHERE database='test_ck' AND engine='View';

-- 确认SR表恢复原名
SHOW TABLES FROM test_sr;

-- 确认CK表字段被删除
DESCRIBE test_ck.test_table;

-- 检查锁获取日志
grep "成功获取锁" logs/cksr.log
```

## 互斥锁测试

### 测试场景：并发操作冲突

**测试步骤：**
1. 启动程序（启用视图更新器）：`./cksr.exe -config config.json &`
2. 立即启动回滚操作：`./cksr.exe -config config.json -rollback`

**预期结果：**
- ✅ 第一个操作获取锁成功
- ✅ 第二个操作等待或失败，显示锁被占用的错误信息
- ✅ 操作不会同时进行，确保数据一致性

## 错误处理测试

### 数据库连接失败
```bash
# 停止数据库服务后测试
./cksr.exe -config config.json
```

**预期结果：**
- ✅ 程序显示连接失败错误
- ✅ 程序优雅退出，不会崩溃

### 配置文件错误
```bash
# 使用错误的配置文件测试
./cksr.exe -config invalid_config.json
```

**预期结果：**
- ✅ 程序显示配置文件解析错误
- ✅ 程序优雅退出

### 权限不足
```bash
# 使用只读用户测试
./cksr.exe -config readonly_config.json
```

**预期结果：**
- ✅ 程序显示权限不足错误
- ✅ 程序记录详细的错误日志

## 性能测试

### 大量视图更新
- 创建100个视图
- 启动视图更新器
- 观察更新性能和资源使用

### 长时间运行稳定性
- 启动程序并运行24小时
- 检查内存泄漏和性能退化

## 测试结果记录

| 场景 | 状态 | 执行时间 | 备注 |
|------|------|----------|------|
| 场景1 | ⏳ 待测试 | - | CK表已添加字段 + 继续运行 |
| 场景2 | ⏳ 待测试 | - | CK表已添加字段 + 回滚 |
| 场景3 | ⏳ 待测试 | - | SR表已改名 + 继续运行 |
| 场景4 | ⏳ 待测试 | - | SR表已改名 + 回滚 |
| 场景5 | ⏳ 待测试 | - | 视图已创建 + 继续运行 |
| 场景6 | ⏳ 待测试 | - | 视图已创建 + 回滚 |

## 注意事项

1. **测试环境隔离**：每个测试场景应在独立的数据库环境中进行
2. **数据备份**：测试前备份重要数据
3. **日志监控**：密切关注程序日志输出
4. **资源监控**：监控CPU、内存使用情况
5. **网络稳定性**：确保数据库连接稳定

## 故障排除

### 常见问题

1. **锁获取失败**
   - 检查是否有其他实例在运行
   - 检查k8s lease资源状态

2. **视图更新失败**
   - 检查数据库连接
   - 检查表结构变化
   - 检查权限设置

3. **cron表达式错误**
   - 验证cron表达式格式
   - 检查时区设置

4. **内存使用过高**
   - 检查视图数量
   - 优化查询语句
   - 调整更新频率