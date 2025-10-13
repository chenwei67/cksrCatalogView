-- 示例表创建SQL文件
-- 这个文件演示了如何使用脚本执行CREATE TABLE语句

CREATE TABLE IF NOT EXISTS business.asset_log 
( 
    `uuId` String COMMENT '日志唯一id', 
    `insertTime` DateTime, 
    `tenant` String COMMENT '租户id', 
    `data` String COMMENT '原始数据' 
) 
ENGINE=OLAP 
DUPLICATE KEY(`uuId`,`insertTime`) 
PARTITION BY RANGE(insertTime)() 
DISTRIBUTED BY HASH(`uuId`) BUCKETS 2 
PROPERTIES ( 
    "dynamic_partition.enable" = "true", 
    "dynamic_partition.time_unit" = "DAY", 
    "dynamic_partition.start" = "-30", 
    "dynamic_partition.history_partition_num" = "7", 
    "dynamic_partition.end" = "7", 
    "dynamic_partition.time_zone" = "Asia/Shanghai", 
    "fast_schema_evolution" = "true", 
    "replicated_storage" = "true", 
    "replication_num" = "1", 
    "compression" = "zstd(3)", 
    "bloom_filter_columns" = "uuId" 
);