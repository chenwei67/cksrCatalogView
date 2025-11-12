-- 在自己的用例目录中准备异常场景：为 SR 创建一个包含额外列的表，制造字段映射不一致
-- 选择与 temp/sqls 中的示例一致的基础名，以确保被 init 处理
CREATE TABLE IF NOT EXISTS business.dns_log (
  id INT,
  recordTimestamp BIGINT,
  extra_col INT
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);