package builder

import (
	"fmt"
	"strings"

	"cksr/parser"
)

// SRPairViewBuilder 基于同库内的新旧两张 StarRocks 表构建兼容视图。
type SRPairViewBuilder struct {
	dbName    string
	viewName  string
	oldTable  parser.Table
	newTable  parser.Table
	oldFields map[string]parser.Field
	newFields map[string]parser.Field
}

func NewSRPairViewBuilder(dbName, viewName string, oldTable, newTable parser.Table) *SRPairViewBuilder {
	return &SRPairViewBuilder{
		dbName:    dbName,
		viewName:  viewName,
		oldTable:  oldTable,
		newTable:  newTable,
		oldFields: indexFieldsByName(oldTable.Field),
		newFields: indexFieldsByName(newTable.Field),
	}
}

func indexFieldsByName(fields []parser.Field) map[string]parser.Field {
	res := make(map[string]parser.Field, len(fields))
	for _, field := range fields {
		res[field.Name] = field
	}
	return res
}

func (b *SRPairViewBuilder) Build() (string, error) {
	if err := b.validate(); err != nil {
		return "", err
	}

	var oldSelectClauses []string
	var newSelectClauses []string
	for _, newField := range b.newTable.Field {
		newSelectClauses = append(newSelectClauses, fmt.Sprintf("    `%s`", newField.Name))

		oldField, exists := b.oldFields[newField.Name]
		if exists {
			oldSelectClauses = append(oldSelectClauses, fmt.Sprintf("    `%s`", oldField.Name))
			continue
		}

		clause, err := buildOldTableCompatibleClause(newField)
		if err != nil {
			return "", err
		}
		oldSelectClauses = append(oldSelectClauses, "    "+clause)
	}

	return fmt.Sprintf(
		"CREATE VIEW `%s`.`%s` AS\nSELECT\n%s\nFROM `%s`.`%s`\nUNION ALL\nSELECT\n%s\nFROM `%s`.`%s`;\n",
		b.dbName,
		b.viewName,
		strings.Join(oldSelectClauses, ",\n"),
		b.dbName,
		b.oldTable.DDL.TableName,
		strings.Join(newSelectClauses, ",\n"),
		b.dbName,
		b.newTable.DDL.TableName,
	), nil
}

func (b *SRPairViewBuilder) validate() error {
	if strings.TrimSpace(b.dbName) == "" {
		return fmt.Errorf("数据库名为空")
	}
	if strings.TrimSpace(b.viewName) == "" {
		return fmt.Errorf("视图名为空")
	}
	if len(b.oldTable.Field) == 0 {
		return fmt.Errorf("旧表 %s 没有可用列", b.oldTable.DDL.TableName)
	}
	if len(b.newTable.Field) == 0 {
		return fmt.Errorf("新表 %s 没有可用列", b.newTable.DDL.TableName)
	}

	for _, oldField := range b.oldTable.Field {
		newField, exists := b.newFields[oldField.Name]
		if !exists {
			return fmt.Errorf("旧表 %s 存在新表 %s 不包含的列 %s", b.oldTable.DDL.TableName, b.newTable.DDL.TableName, oldField.Name)
		}
		if !strings.EqualFold(strings.TrimSpace(oldField.Type), strings.TrimSpace(newField.Type)) {
			return fmt.Errorf("列 %s 类型不一致: 旧表=%s, 新表=%s", oldField.Name, oldField.Type, newField.Type)
		}
	}

	if len(b.newTable.Field) <= len(b.oldTable.Field) {
		return fmt.Errorf("新表 %s 的列数(%d)未多于旧表 %s 的列数(%d)", b.newTable.DDL.TableName, len(b.newTable.Field), b.oldTable.DDL.TableName, len(b.oldTable.Field))
	}
	return nil
}

func buildOldTableCompatibleClause(newField parser.Field) (string, error) {
	switch {
	case strings.EqualFold(strings.TrimSpace(newField.DefaultKind), "AS") && strings.TrimSpace(newField.DefaultExpr) != "":
		return fmt.Sprintf("CAST(%s AS %s) AS `%s`", newField.DefaultExpr, newField.Type, newField.Name), nil
	case strings.EqualFold(strings.TrimSpace(newField.DefaultKind), "DEFAULT") && strings.TrimSpace(newField.DefaultExpr) != "":
		return fmt.Sprintf("CAST(%s AS %s) AS `%s`", newField.DefaultExpr, newField.Type, newField.Name), nil
	default:
		return "", fmt.Errorf("新列 %s 无法在旧表侧补齐: 既不是生成列也没有默认值", newField.Name)
	}
}
