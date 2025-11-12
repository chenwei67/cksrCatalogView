-- 在自己的用例目录中准备异常场景：为 SR 创建一个包含额外列的表，制造字段映射不一致
-- 选择与 temp/sqls 中的示例一致的基础名，以确保被 init 处理
CREATE TABLE IF NOT EXISTS business.dns_log (
  id INT,
  recordTimestamp BIGINT,
  extra_col INT,
  -- 新增：数组类型列（测试SR-only数组列的默认占位行为）
  tags ARRAY<STRING>,
  -- 新增：带默认值的列（测试默认值被沿用）
  status INT DEFAULT "1"
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);