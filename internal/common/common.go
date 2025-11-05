package common

import (
    "fmt"
    "time"

    "cksr/config"
    "cksr/logger"
    "cksr/parser"
)

// 角色常量，统一身份后缀
const (
    RoleUpdater  = "updater"
    RoleRollback = "rollback"
)

// ParseTableFromString 从DDL字符串解析表结构，并设置正确的数据库名和表名（带可配置超时）
func ParseTableFromString(ddl string, dbName string, tableName string, cfg *config.Config) (parser.Table, error) {
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
    case <-time.After(time.Duration(cfg.Parser.DDLParseTimeoutSeconds) * time.Second):
        logger.Warn("DDL解析超时 (%d秒)", cfg.Parser.DDLParseTimeoutSeconds)
        return parser.Table{}, fmt.Errorf("DDL解析超时")
    }
}

// BuildIdentity 为锁身份追加角色后缀，统一规则
func BuildIdentity(base string, role string) string {
    if base == "" {
        base = "cksr-instance"
    }
    if role == "" {
        return base
    }
    return base + "-" + role
}