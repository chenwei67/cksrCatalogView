package builder

import (
	"fmt"
	"maps"
	"strings"

	"cksr/parser"
)

type ViewBuilder struct {
	ck       CKTableBuilder
	sr       SRTableBuilder
	viewName string // view的名称，就是ck中的name名称
	dbName   string // 数据库名称，应该就是sr中的db
}

type CKField struct {
	FieldConverter
	SRField SRField
	Clause  string
}

func NewCKField(c FieldConverter) CKField {
	return CKField{
		FieldConverter: c,
	}
}

func (c *CKField) SetSRField(field SRField) {
	c.SRField = field
}

type SRField struct {
	parser.Field
	Clause string // select 语句中的子句
}

func (sf *SRField) GenClause() {
	sf.Clause = sf.Field.Name
}

type TableBuilder struct {
	DBName string
	Name   string
}

// 构建ck的select
type CKTableBuilder struct {
	TableBuilder
	catalogName string
	converters  []FieldConverter // 顺序就是select的列顺序
	fields      []CKField
}

// 构建sr的select
type SRTableBuilder struct {
	TableBuilder
	fields  []SRField
	nameMap map[string]SRField
}

func (st *SRTableBuilder) addClauseField(srField SRField) {
	st.fields = append(st.fields, srField)
}

func (ct *CKTableBuilder) addClauseField(ckField CKField) {
	ct.fields = append(ct.fields, ckField)
}

func NewCKTableBuilder(fieldConverters []FieldConverter, tableName, dbName, catalogName string) CKTableBuilder {
	return CKTableBuilder{
		TableBuilder: TableBuilder{
			DBName: dbName,
			Name:   tableName,
		},
		converters:  fieldConverters,
		catalogName: catalogName,
	}
}

func NewSRTableBuilder(fields []parser.Field, tableName, dbName string) SRTableBuilder {
	m := make(map[string]SRField)
	for _, f := range fields {
		// 忽略syncFromCK字段，这是StarRocks专用的标识字段，不参与视图映射
		if f.Name == "syncFromCK" {
			continue
		}
		m[f.Name] = SRField{
			Field: f,
		}
	}
	return SRTableBuilder{
		TableBuilder: TableBuilder{
			DBName: dbName,
			Name:   tableName,
		},
		nameMap: m,
	}
}

func (ct *CKTableBuilder) GenQuerySQL() string {
	var fieldsClause []string
	for i, f := range ct.fields {
		var clause string
		if i == len(ct.fields)-1 {
			clause = fmt.Sprintf("\t%s \n", f.Clause)
		} else {
			clause = fmt.Sprintf("\t%s, \n", f.Clause)
		}
		fieldsClause = append(fieldsClause, clause)
	}
	clauses := strings.Join(fieldsClause, "")
	return fmt.Sprintf("select \n %sfrom %s.%s.%s", clauses, ct.catalogName, ct.DBName, ct.Name)
}

func (st *SRTableBuilder) GenQuerySQL() string {
	var fieldsClause []string
	for i, f := range st.fields {
		var clause string
		if i == len(st.fields)-1 {
			clause = fmt.Sprintf("\t%s \n", f.Clause)
		} else {
			clause = fmt.Sprintf("\t%s, \n", f.Clause)
		}
		fieldsClause = append(fieldsClause, clause)
	}
	clauses := strings.Join(fieldsClause, "")
	return fmt.Sprintf("select \n %s from %s.%s \n where  syncFromCK = \"false\"", clauses, st.DBName, st.Name)
}

func NewBuilder(
	fieldConverters []FieldConverter,
	srFields []parser.Field,
	ckDBName, ckTableName, ckCatalogName, srDBName, srTableName string) ViewBuilder {
	ckTb := NewCKTableBuilder(fieldConverters, ckTableName, ckDBName, ckCatalogName)
	srTb := NewSRTableBuilder(srFields, srTableName, srDBName)
	return ViewBuilder{
		ck:       ckTb,
		sr:       srTb,
		viewName: ckTableName,
		dbName:   srDBName,
	}
}

// 遍历ck的fields，根据type，决定要不要，如果不要，继续下一个，如果要，如下
// 获取重定向字段，构造出clause，写入tablebuilder；找到sr中对应的字段，同样构造出clause，写入tablebuilder
func (v *ViewBuilder) Build() (string, error) {
	for _, fieldConverter := range v.ck.converters {
		ckField := NewCKField(fieldConverter)
		if ckField.Ignore() {
			continue
		}
		srField, err := v.MapSRField(fieldConverter, v.sr.nameMap)
		if err != nil {
			return "", err
		}

		srField.GenClause()
		v.sr.addClauseField(srField)

		ckField.SetSRField(srField)
		ckField.GenClause()
		v.ck.addClauseField(ckField)
	}
	if len(v.ck.fields) == 0 {
		return "", fmt.Errorf("ck field is empty")
	}
	if len(v.sr.fields) != len(v.sr.nameMap) {
		var err error
		var fs []SRField
		nameMapCopy := make(map[string]SRField)
		maps.Copy(nameMapCopy, v.sr.nameMap)
		for _, f := range v.sr.fields {
			if _, ok := v.sr.nameMap[f.Name]; !ok {
				fs = append(fs, f)
			} else {
				delete(nameMapCopy, f.Name)
			}
		}
		if len(fs) != 0 {
			err = fmt.Errorf("build select column in view error, some fields not exists in create sql ddl: %+v", fs)
		}
		if len(nameMapCopy) != 0 {
			var lackfs []SRField
			for _, f := range nameMapCopy {
				lackfs = append(lackfs, f)
			}
			if err != nil {
				err = fmt.Errorf("%w, and some fields exists in create sql ddl but not in view select: %+v", err, lackfs)
			} else {
				err = fmt.Errorf("build select column in view error, some fields exists in create sql ddl but not in view select: %+v", lackfs)
			}
		}
		return "", err
	}

	ckQ := v.ck.GenQuerySQL()
	srQ := v.sr.GenQuerySQL()
	return v.GenViewSQL(ckQ, srQ), nil
}

func (v *ViewBuilder) GenViewSQL(ckQ, srQ string) string {
	return fmt.Sprintf("create view if not exists %s.%s as \n%s \nunion all \n%s; \n", v.dbName, v.viewName, ckQ, srQ)
}

// GenViewSQLWithIfNotExists 生成带IF NOT EXISTS的CREATE VIEW语句
// 这个方法与GenViewSQL功能相同，但提供了更明确的语义
func (v *ViewBuilder) GenViewSQLWithIfNotExists(ckQ, srQ string) string {
	return v.GenViewSQL(ckQ, srQ)
}

// 是rowLogAlias
func (f *CKField) Ignore() bool {
	n := strings.ToLower(f.originName())
	return n == "rowlogalias" || n == "_invert_text"
}

// 构建clause
func (f *CKField) GenClause() {
	// 开始构建
	if IsStringArray(f.originType()) {
		f.Clause = f.Array()
	} else if IsArray(f.originType()) {
		f.Clause = f.ArrayMap()
	} else {
		f.Clause = f.Name
	}
}

func (f *CKField) ArrayMap() string {
	return fmt.Sprintf("CASE \n\t\tWHEN %s = '' THEN %s[]\n\t\tELSE array_map(x -> CAST(x AS %s), split(%s, 'CKTOSRFRAGEMENT'))\n\tEND as %s", f.Name, f.SRField.Type, f.SRBasicType(), f.Name, f.SRField.Name)
}

func (f *CKField) Array() string {
	return fmt.Sprintf("CASE \n\t\tWHEN %s = '' THEN ARRAY<String>[]\n\t\tELSE split(%s, 'CKTOSRFRAGEMENT')\n\tEND as %s", f.Name, f.Name, f.SRField.Name)
}

func (f *CKField) SRBasicType() string {
	t := f.SRField.Type
	var basicType []byte
	for i := 0; i < len(t); i++ {
		if t[i] == '<' {
			basicType = []byte{}
		} else if t[i] == '>' {
			return string(basicType)
		} else {
			basicType = append(basicType, t[i])
		}
	}
	return t
}

// 映射到sr字段
func (v *ViewBuilder) MapSRField(field FieldConverter, srNameFieldMap map[string]SRField) (SRField, error) {
	name := field.originName()
	if IsIPV6(field.originType()) || IsIPV4(field.originType()) {
		name = fmt.Sprintf("%s_int", field.originName())
	}

	if v, ok := srNameFieldMap[name]; ok {
		return v, nil
	} else {
		return SRField{}, fmt.Errorf("map failed, column %s not exists in sr", name)
	}
}
