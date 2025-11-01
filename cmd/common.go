package cmd

import (
    "fmt"
    "time"

    "cksr/logger"
    "cksr/parser"
)

// parseTableFromString 从DDL字符串解析表结构，并设置正确的数据库名和表名
func parseTableFromString(ddl string, dbName string, tableName string) (parser.Table, error) {
    logger.Debug("完整DDL内容:\n%s", ddl)
    logger.Debug("DDL内容结束")

    done := make(chan parser.Table, 1)
    go func() {
        logger.Debug("调用ParserTableSQL函数...")
        table := parser.ParserTableSQL(ddl)
        logger.Debug("ParserTableSQL函数执行完成")
        done <- table
    }()

    select {
    case table := <-done:
        logger.Debug("DDL解析成功")
        if dbName != "" {
            table.DDL.DBName = dbName
        }
        if tableName != "" {
            table.DDL.TableName = tableName
        }
        logger.Debug("设置数据库名: %s, 表名: %s", dbName, tableName)
        return table, nil
    case <-time.After(60 * time.Second):
        logger.Warn("DDL解析超时 (60秒)")
        return parser.Table{}, fmt.Errorf("DDL解析超时")
    }
}