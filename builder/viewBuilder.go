package builder

import (
	"database/sql"
	"fmt"
	"maps"
	"strings"

	"cksr/logger"
	"cksr/parser"
	"cksr/retry"
)

// DatabaseManager 定义数据库管理器接口（简化版，只需要获取连接）
type DatabaseManager interface {
	GetStarRocksConnection() (*sql.DB, error)
}

type ViewBuilder struct {
	ck        CKTableBuilder
	sr        SRTableBuilder
	viewName  string          // view的名称，就是ck中的name名称
	dbName    string          // 数据库名称，应该就是sr中的db
	dbManager DatabaseManager // 数据库管理器，用于执行查询
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
	sf.Clause = fmt.Sprintf("`%s`", sf.Field.Name)
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
	logger.Debug("添加StarRocks字段到fields: %s", srField.Name)

	// 检查是否已存在同名字段
	for i, existingField := range st.fields {
		if existingField.Name == srField.Name {
			logger.Warn("发现重复的StarRocks字段名: %s (索引: %d)", srField.Name, i)
		}
	}

	st.fields = append(st.fields, srField)
	logger.Debug("当前StarRocks fields数量: %d, nameMap数量: %d", len(st.fields), len(st.nameMap))
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
	logger.Debug("创建SRTableBuilder，输入字段数量: %d", len(fields))

	m := make(map[string]SRField)
	duplicateCount := 0

	for i, f := range fields {
		logger.Debug("处理StarRocks字段 #%d: %s (类型: %s)", i+1, f.Name, f.Type)

		// 检查是否已存在同名字段
		if _, exists := m[f.Name]; exists {
			logger.Warn("发现重复的字段名: %s，将覆盖之前的定义", f.Name)
			duplicateCount++
		}

		m[f.Name] = SRField{
			Field: f,
		}
	}

	logger.Debug("SRTableBuilder创建完成 - 总字段: %d, 重复: %d, 最终nameMap数量: %d",
		len(fields), duplicateCount, len(m))

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
	return fmt.Sprintf("select \n %s from %s.%s \n", clauses, st.DBName, st.Name)
}

func NewBuilder(
	fieldConverters []FieldConverter,
	srFields []parser.Field,
	ckDBName, ckTableName, ckCatalogName, srDBName, srTableName string,
	dbManager DatabaseManager) ViewBuilder {
	ckTb := NewCKTableBuilder(fieldConverters, ckTableName, ckDBName, ckCatalogName)
	srTb := NewSRTableBuilder(srFields, srTableName, srDBName)
	return ViewBuilder{
		ck:        ckTb,
		sr:        srTb,
		viewName:  ckTableName,
		dbName:    srDBName,
		dbManager: dbManager,
	}
}

// 遍历ck的fields，根据type，决定要不要，如果不要，继续下一个，如果要，如下
// 获取重定向字段，构造出clause，写入tablebuilder；找到sr中对应的字段，同样构造出clause，写入tablebuilder
func (v *ViewBuilder) Build() (string, error) {
	logger.Debug("开始构建视图 %s.%s", v.dbName, v.viewName)
	logger.Debug("ClickHouse表: %s.%s (catalog: %s)", v.ck.DBName, v.ck.Name, v.ck.catalogName)
	logger.Debug("StarRocks表: %s.%s", v.sr.DBName, v.sr.Name)
	logger.Debug("ClickHouse字段转换器数量: %d", len(v.ck.converters))
	logger.Debug("StarRocks字段映射数量: %d", len(v.sr.nameMap))

	processedFields := 0
	skippedFields := 0

	for i, fieldConverter := range v.ck.converters {
		if i > 0 && i%100 == 0 {
			logger.Debug("ViewBuilder字段处理进度: %d/%d", i, len(v.ck.converters))
		}

		logger.Debug("处理ClickHouse字段 #%d: %s (类型: %s)", i+1, fieldConverter.originName(), fieldConverter.originType())

		ckField := NewCKField(fieldConverter)
		// if ckField.Ignore() {
		// 	continue
		// }

		logger.Debug("开始映射StarRocks字段...")
		srField, err := v.MapSRField(fieldConverter, v.sr.nameMap)
		if err != nil {
			// 如果ClickHouse字段在StarRocks中不存在，跳过该字段而不是报错
			logger.Warn("ClickHouse字段 '%s' 在StarRocks中不存在，跳过该字段", fieldConverter.originName())
			skippedFields++
			continue
		}
		logger.Debug("成功映射到StarRocks字段: %s", srField.Name)

		logger.Debug("生成StarRocks字段子句...")
		srField.GenClause()
		v.sr.addClauseField(srField)

		logger.Debug("设置ClickHouse字段的StarRocks映射...")
		ckField.SetSRField(srField)
		logger.Debug("生成ClickHouse字段子句...")
		ckField.GenClause()
		v.ck.addClauseField(ckField)

		processedFields++
		logger.Debug("字段 %s 处理完成", fieldConverter.originName())
	}

	logger.Debug("字段处理完成 - 总数: %d, 处理: %d, 跳过: %d", len(v.ck.converters), processedFields, skippedFields)
	logger.Debug("最终映射的字段数量 - ClickHouse: %d, StarRocks: %d", len(v.ck.fields), len(v.sr.fields))

	if len(v.ck.fields) == 0 {
		logger.Error("ClickHouse字段为空，无法创建视图")
		return "", fmt.Errorf("ck field is empty")
	}

	// 添加详细的字段映射验证日志
	logger.Debug("开始字段映射验证 - StarRocks字段总数: %d, 已映射字段数: %d", len(v.sr.nameMap), len(v.sr.fields))

	// 添加字段名称统计
	fieldNames := make(map[string]int)
	for _, field := range v.sr.fields {
		fieldNames[field.Name]++
	}

	duplicateFieldNames := make([]string, 0)
	for name, count := range fieldNames {
		if count > 1 {
			duplicateFieldNames = append(duplicateFieldNames, fmt.Sprintf("%s(x%d)", name, count))
		}
	}

	if len(duplicateFieldNames) > 0 {
		logger.Error("发现重复的字段名: %v", duplicateFieldNames)
	}

	logger.Debug("字段名称统计完成 - 唯一字段名: %d, 重复字段名: %d", len(fieldNames), len(duplicateFieldNames))

	if len(v.sr.fields) != len(v.sr.nameMap) {
		var err error
		var fs []SRField
		nameMapCopy := make(map[string]SRField)
		maps.Copy(nameMapCopy, v.sr.nameMap)

		logger.Debug("检查字段映射一致性...")
		for i, f := range v.sr.fields {
			logger.Debug("检查字段 #%d: %s", i+1, f.Name)
			if _, ok := v.sr.nameMap[f.Name]; !ok {
				logger.Warn("字段 %s 在nameMap中不存在", f.Name)
				fs = append(fs, f)
			} else {
				delete(nameMapCopy, f.Name)
			}
		}

		if len(fs) != 0 {
			logger.Error("发现不存在于DDL中的字段: %+v", fs)
			err = fmt.Errorf("build select column in view error, some fields not exists in create sql ddl: %+v", fs)
		}
		if len(nameMapCopy) != 0 {
			var lackfs []SRField
			for _, f := range nameMapCopy {
				lackfs = append(lackfs, f)
			}
			logger.Error("发现存在于DDL但未映射的字段: %+v", lackfs)
			if err != nil {
				err = fmt.Errorf("%w, and some fields exists in create sql ddl but not in view select: %+v", err, lackfs)
			} else {
				err = fmt.Errorf("build select column in view error, some fields exists in create sql ddl but not in view select: %+v", lackfs)
			}
		}

		// 如果err仍然为nil，说明字段数量不匹配但没有具体的错误字段，这是一个异常情况
		if err == nil {
			err = fmt.Errorf("字段映射数量不匹配: sr.fields数量=%d, sr.nameMap数量=%d, 但未发现具体的不匹配字段", len(v.sr.fields), len(v.sr.nameMap))
		}

		logger.Error("字段映射验证失败: %v", err)
		return "", err
	}

	logger.Debug("字段映射验证通过，开始生成SQL")

	ckQ := v.ck.GenQuerySQL()
	srQ := v.sr.GenQuerySQL()
	logger.Debug("生成的ClickHouse查询SQL:\n%s", ckQ)
	logger.Debug("生成的StarRocks查询SQL:\n%s", srQ)

	viewSQL, err := v.GenViewSQL(ckQ, srQ)
	if err != nil {
		return "", fmt.Errorf("生成视图SQL失败: %w", err)
	}
	logger.Debug("生成的CREATE VIEW SQL:\n%s", viewSQL)

	return viewSQL, nil
}

func (v *ViewBuilder) GenViewSQL(ckQ, srQ string) (string, error) {
	logger.Debug("开始生成视图SQL")
	logger.Debug("ClickHouse查询SQL: %s", ckQ)
	logger.Debug("StarRocks查询SQL: %s", srQ)

	// 先执行子查询获取固定的时间值
	minTimestampQuery := fmt.Sprintf("select min(recordTimestamp) from %s.%s", v.sr.DBName, v.sr.Name)
	logger.Debug("执行子查询获取最小时间戳: %s", minTimestampQuery)

	// 查询最小时间戳，使用通用的重试wrapper
	var nullableTimestamp *int64
	var minTimestamp string
	
	db, err := v.dbManager.GetStarRocksConnection()
	if err != nil {
		return "", fmt.Errorf("获取StarRocks连接失败: %w", err)
	}
	defer db.Close()
	
	err = retry.QueryRowAndScanWithRetryDefault(db, minTimestampQuery, []interface{}{&nullableTimestamp})
	if err != nil {
		// 检查是否是没有数据的错误
		if err == sql.ErrNoRows {
			logger.Warn("表中没有数据，使用最大默认值")
			minTimestamp = "9999999999999"
		} else {
			return "", fmt.Errorf("查询最小时间戳失败: %w", err)
		}
	} else if nullableTimestamp == nil {
		logger.Warn("查询最小时间戳结果为NULL，使用最大默认值")
		minTimestamp = "9999999999999"
	} else {
		minTimestamp = fmt.Sprintf("%d", *nullableTimestamp)
		logger.Debug("获取到最小时间戳: %s", minTimestamp)
	}

	// 生成视图SQL，ClickHouse使用 < 条件，StarRocks使用 >= 条件
	sql := fmt.Sprintf("create view if not exists %s.%s as \n%s \nwhere recordTimestamp < %s \nunion all \n%s \nwhere recordTimestamp >= %s; \n",
		v.dbName, v.viewName, ckQ, minTimestamp, srQ, minTimestamp)

	logger.Debug("最终视图SQL:\n%s", sql)
	return sql, nil
}

// 是rowLogAlias
func (f *CKField) Ignore() bool {
	n := strings.ToLower(f.originName())
	return n == "rowlogalias" || n == "_invert_text"
}

// 构建clause
func (f *CKField) GenClause() {
	// 开始构建
	if IsArrayIPV6(f.originType()) {
		f.Clause = f.ArrayIPV6()
	} else if IsStringArray(f.originType()) {
		f.Clause = f.Array()
	} else if IsArray(f.originType()) {
		f.Clause = f.ArrayMap()
	} else if f.IsAddedColumn() {
		f.Clause = fmt.Sprintf("`%s` as `%s`", f.Name, f.SRField.Name)
	} else {
		f.Clause = fmt.Sprintf("`%s`", f.Name)
	}
}

func (f *CKField) ArrayMap() string {
	return fmt.Sprintf("CASE \n\t\tWHEN `%s` = '' THEN %s[]\n\t\tELSE array_map(x -> CAST(x AS %s), split(`%s`, 'CKTOSRFRAGEMENT'))\n\tEND as `%s`", f.Name, f.SRField.Type, f.SRBasicType(), f.Name, f.SRField.Name)
}

func (f *CKField) Array() string {
	return fmt.Sprintf("CASE \n\t\tWHEN `%s` = '' THEN ARRAY<String>[]\n\t\tELSE split(`%s`, 'CKTOSRFRAGEMENT')\n\tEND as `%s`", f.Name, f.Name, f.SRField.Name)
}

func (f *CKField) ArrayIPV6() string {
	return fmt.Sprintf("CASE \n\t\tWHEN `%s` = '' THEN ARRAY<LARGEINT>[]\n\t\tELSE array_map(x -> CAST(x AS LARGEINT), split(`%s`, 'CKTOSRFRAGEMENT'))\n\tEND as `%s`", f.Name, f.Name, f.SRField.Name)
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
	if IsArrayIPV6(field.originType()) || IsArrayIPV4(field.originType()) {
		name = fmt.Sprintf("%s_int", field.originName())
	} else if IsIPV6(field.originType()) || IsIPV4(field.originType()) {
		name = fmt.Sprintf("%s_int", field.originName())
	}

	if v, ok := srNameFieldMap[name]; ok {
		return v, nil
	} else {
		return SRField{}, fmt.Errorf("map failed, column %s not exists in sr", name)
	}
}
