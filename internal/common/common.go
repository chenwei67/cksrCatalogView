package common

import (
	"cksr/logger"
	"fmt"
	"time"

	p "example.com/migrationLib/parser"
)

// 角色常量，统一身份后缀
const (
	RoleUpdater  = "updater"
	RoleRollback = "rollback"
)

func ParseTableFromString(ddl string, dbName string, tableName string, timeout time.Duration) (p.Table, error) {
	logger.Debug("完整DDL内容:\n%s", ddl)
	logger.Debug("DDL内容结束")
	done := make(chan p.Table, 1)
	go func() {
		t := p.ParserTableSQL(ddl)
		done <- t
	}()
	select {
	case table := <-done:
		if dbName != "" {
			table.DDL.DBName = dbName
		}
		if tableName != "" {
			table.DDL.TableName = tableName
		}
		return table, nil
	case <-time.After(timeout):
		logger.Warn("DDL解析超时 (%s)", timeout.String())
		return p.Table{}, fmt.Errorf("DDL解析超时")
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
