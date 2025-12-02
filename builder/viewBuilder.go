package builder

import (
	"database/sql"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"cksr/config"
	"cksr/logger"
	"cksr/parser"

	ckc "example.com/migrationLib/convert"
	"example.com/migrationLib/retry"
)

// 视图SQL类型常量，避免使用魔字符串
const (
	SQLTypeCreate = "CREATE"
	SQLTypeAlter  = "ALTER"
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
	config    *config.Config  // 配置对象，用于获取时间戳列配置
}

type CKField struct {
	ckc.FieldConverter
	SRField SRField
	Clause  string
}

func NewCKField(c ckc.FieldConverter) CKField {
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
	converters  []ckc.FieldConverter
	fields      []CKField
}

// 构建sr的select
type SRTableBuilder struct {
	TableBuilder
	fields  []SRField          // ck中存在对应的sr的字段
	nameMap map[string]SRField // sr中所有字段，包括了在ck中完全没有对应的
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

func NewCKTableBuilder(fieldConverters []ckc.FieldConverter, tableName, dbName, catalogName string) CKTableBuilder {
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
	fieldConverters []ckc.FieldConverter,
	srFields []parser.Field,
	ckDBName, ckTableName, ckCatalogName, srDBName, srTableName string,
	dbManager DatabaseManager,
	cfg *config.Config) ViewBuilder {
	ckTb := NewCKTableBuilder(fieldConverters, ckTableName, ckDBName, ckCatalogName)
	srTb := NewSRTableBuilder(srFields, srTableName, srDBName)
	return ViewBuilder{
		ck:        ckTb,
		sr:        srTb,
		viewName:  ckTableName,
		dbName:    srDBName,
		dbManager: dbManager,
		config:    cfg,
	}
}

// 遍历ck的fields，根据type，决定要不要，如果不要，继续下一个，如果要，如下
// 获取重定向字段，构造出clause，写入tablebuilder；找到sr中对应的字段，同样构造出clause，写入tablebuilder
func (v *ViewBuilder) Build() (string, error) {
	return v.BuildWithType(SQLTypeCreate)
}

// BuildWithType 生成视图SQL，支持 CREATE 或 ALTER
func (v *ViewBuilder) BuildWithType(sqlType string) (string, error) {
	logger.Debug("开始构建视图 %s.%s", v.dbName, v.viewName)
	logger.Debug("ClickHouse表: %s.%s (catalog: %s)", v.ck.DBName, v.ck.Name, v.ck.catalogName)
	logger.Debug("StarRocks表: %s.%s", v.sr.DBName, v.sr.Name)
	logger.Debug("ClickHouse字段转换器数量: %d", len(v.ck.converters))
	logger.Debug("StarRocks字段映射数量: %d", len(v.sr.nameMap))
	// 统一执行映射与严格校验
	if err := v.PrepareAndValidate(); err != nil {
		return "", err
	}

	ckQ := v.ck.GenQuerySQL()
	srQ := v.sr.GenQuerySQL()
	logger.Debug("生成的ClickHouse查询SQL:\n%s", ckQ)
	logger.Debug("生成的StarRocks查询SQL:\n%s", srQ)

	viewSQL, err := v.GenViewSQLWithType(ckQ, srQ, sqlType)
	if err != nil {
		return "", fmt.Errorf("生成视图SQL失败: %w", err)
	}
	logger.Debug("生成的VIEW SQL:\n%s", viewSQL)

	return viewSQL, nil
}

// PrepareAndValidate 执行字段映射并进行严格校验（可被多处复用）
func (v *ViewBuilder) PrepareAndValidate() error {
	// 重置已生成的字段，避免重复构建
	v.ck.fields = nil
	v.sr.fields = nil

	processedFields := 0
	skippedFields := 0

	for i, fieldConverter := range v.ck.converters {
		if i > 0 && i%100 == 0 {
			logger.Debug("ViewBuilder字段处理进度: %d/%d", i, len(v.ck.converters))
		}

		logger.Debug("处理ClickHouse字段 #%d: %s (类型: %s)", i+1, fieldConverter.OriginName(), fieldConverter.OriginType())

		ckField := NewCKField(fieldConverter)

		logger.Debug("开始映射StarRocks字段...")
		srField, err := v.MapSRField(fieldConverter, v.sr.nameMap)
		if err != nil {
			// 如果ClickHouse字段在StarRocks中不存在，跳过该字段而不是报错
			logger.Warn("ClickHouse字段 '%s' 在StarRocks中不存在，跳过该字段", fieldConverter.OriginName())
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
		logger.Debug("字段 %s 处理完成", fieldConverter.OriginName())
	}

	// 处理 SR 独有列：在 CK 子查询中补默认值占位，保证两侧列/类型一致
	{
		// 统计已映射的 SR 列名
		mapped := make(map[string]bool)
		for _, f := range v.sr.fields {
			mapped[f.Name] = true
		}

		// 遍历 SR DDL 中的所有列，找出未映射的列
		for name, sf := range v.sr.nameMap {
			if mapped[name] {
				continue
			}

			logger.Warn("StarRocks 字段 '%s' 在 ClickHouse 中不存在，使用默认值在CK侧补列", name)

			// SR 子句补充（保持视图两侧列顺序一致）
			sf.GenClause()
			v.sr.addClauseField(sf)

			// CK 子句补充默认值占位，并别名为 SR 字段名
			defaultClause := v.defaultCKClauseForSRField(sf)
			ckField := CKField{}
			ckField.SRField = sf
			ckField.Clause = defaultClause
			v.ck.addClauseField(ckField)
		}
	}

	logger.Debug("字段处理完成 - 总数: %d, 处理: %d, 跳过: %d", len(v.ck.converters), processedFields, skippedFields)
	logger.Debug("最终映射的字段数量 - ClickHouse: %d, StarRocks: %d", len(v.ck.fields), len(v.sr.fields))

	if len(v.ck.fields) == 0 {
		logger.Error("ClickHouse字段为空，无法创建视图")
		return fmt.Errorf("ck field is empty")
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
		return err
	}

	logger.Debug("字段映射验证通过")
	return nil
}

// defaultCKClauseForSRField 为 SR 独有列生成 CK 子查询中的默认值占位表达式
// 语义：将 CK 侧该列视为 SR 列的“默认空值”（统一使用 CAST NULL 保持类型一致；数组使用空数组字面量）
func (v *ViewBuilder) defaultCKClauseForSRField(sf SRField) string {
	t := strings.TrimSpace(sf.Type)
	//upper := strings.ToUpper(t)

	// 如果SR列声明了DEFAULT，则在CK子查询侧优先使用该默认值，并强制类型对齐
	if strings.EqualFold(sf.DefaultKind, "DEFAULT") && strings.TrimSpace(sf.DefaultExpr) != "" {
		return fmt.Sprintf("CAST(%s AS %s) as `%s`", sf.DefaultExpr, t, sf.Name)
	}

	// 无默认值：统一用 NULL，占位并按 SR 列类型对齐
	return fmt.Sprintf("CAST(NULL AS %s) as `%s`", t, sf.Name)
}

// BuildAlter 生成 ALTER VIEW SQL（便捷方法）
func (v *ViewBuilder) BuildAlter() (string, error) {
	return v.BuildWithType(SQLTypeAlter)
}

// BuildAlterWithPartition 使用提供的分区时间值生成 ALTER VIEW SQL
// partitionValue: 原始字符串形式的时间值
// isNumeric: 为true则按数值直接使用；为false则按字符串加引号
func (v *ViewBuilder) BuildAlterWithPartition(partitionValue string) (string, error) {
	logger.Debug("开始构建带分区值的ALTER VIEW，值: %s", partitionValue)
	// 强制执行完整的字段映射与校验逻辑，保持与 BuildWithType 一致
	if err := v.PrepareAndValidate(); err != nil {
		return "", err
	}

	// 在完成校验后再生成查询SQL，避免绕过校验
	ckQ := v.ck.GenQuerySQL()
	srQ := v.sr.GenQuerySQL()
	logger.Debug("生成的ClickHouse查询SQL:\n%s", ckQ)
	logger.Debug("生成的StarRocks查询SQL:\n%s", srQ)

	// 获取时间戳列信息
	timestampColumn := v.getTimestampColumnName(v.sr.Name)
	timestampType := strings.ToLower(v.getTimestampColumnType(v.sr.Name))

	// 严格校验并规范化分区值：
	// - datetime/date 类型：必须为可解析的时间字符串（不接受纯数字）；最终以单引号包裹
	// - bigint 类型：必须为纯数字；直接使用数值
	// 规范化原始输入
	trimmed := strings.Trim(partitionValue, "'")

	var minTimestamp string
	switch timestampType {
	case "datetime":
		if _, err := time.Parse("2006-01-02 15:04:05", trimmed); err != nil {
			return "", fmt.Errorf("分区值解析失败：%v（期望格式 YYYY-MM-DD HH:MM:SS）", err)
		}
		minTimestamp = "'" + trimmed + "'"
	case "date":
		if _, err := time.Parse("2006-01-02", trimmed); err != nil {
			return "", fmt.Errorf("分区值解析失败：%v（期望格式 YYYY-MM-DD）", err)
		}
		minTimestamp = "'" + trimmed + "'"
	case "bigint":
		if _, err := strconv.ParseInt(trimmed, 10, 64); err != nil {
			return "", fmt.Errorf("分区值类型不匹配：列类型为 bigint，分区值必须为纯数字")
		}
		minTimestamp = trimmed
	default:
		return "", fmt.Errorf("不支持的时间戳列类型：%s，仅支持 date、datetime、bigint", timestampType)
	}

	sql := v.ComposeFinalSQL(SQLTypeAlter, ckQ, srQ, timestampColumn, minTimestamp)
	logger.Debug("最终视图SQL(带分区值):\n%s", sql)
	return sql, nil
}

// getTimestampColumnName 根据配置获取指定表的时间戳列名，如果没有配置则返回默认的recordTimestamp
func (v *ViewBuilder) getTimestampColumnName(tableName string) string {
	if v.config != nil && v.config.TimestampColumns != nil {
		// 首先尝试使用完整表名查找配置
		if columnConfig, exists := v.config.TimestampColumns[tableName]; exists {
			logger.Debug("表 %s 使用自定义时间戳列名: %s", tableName, columnConfig.Column)
			return columnConfig.Column
		}

		// 如果没有找到配置，尝试去除可能的表后缀后再查找
		// 遍历所有数据库对，检查是否有匹配的后缀
		for _, pair := range v.config.DatabasePairs {
			if pair.SRTableSuffix != "" && strings.HasSuffix(tableName, pair.SRTableSuffix) {
				originalTableName := strings.TrimSuffix(tableName, pair.SRTableSuffix)
				if columnConfig, exists := v.config.TimestampColumns[originalTableName]; exists {
					logger.Debug("表 %s (去除后缀 %s 后为 %s) 使用自定义时间戳列名: %s", tableName, pair.SRTableSuffix, originalTableName, columnConfig.Column)
					return columnConfig.Column
				}
			}
		}
	}
	logger.Debug("表 %s 使用默认时间戳列名: recordTimestamp", tableName)
	return "recordTimestamp"
}

// getTimestampColumnType 根据配置获取指定表的时间戳列数据类型，如果没有配置则返回默认的bigint
func (v *ViewBuilder) getTimestampColumnType(tableName string) string {
	if v.config != nil && v.config.TimestampColumns != nil {
		// 首先尝试使用完整表名查找配置
		if columnConfig, exists := v.config.TimestampColumns[tableName]; exists {
			logger.Debug("表 %s 使用自定义时间戳列类型: %s", tableName, columnConfig.Type)
			return columnConfig.Type
		}

		// 如果没有找到配置，尝试去除可能的表后缀后再查找
		// 遍历所有数据库对，检查是否有匹配的后缀
		for _, pair := range v.config.DatabasePairs {
			if pair.SRTableSuffix != "" && strings.HasSuffix(tableName, pair.SRTableSuffix) {
				originalTableName := strings.TrimSuffix(tableName, pair.SRTableSuffix)
				if columnConfig, exists := v.config.TimestampColumns[originalTableName]; exists {
					logger.Debug("表 %s (去除后缀 %s 后为 %s) 使用自定义时间戳列类型: %s", tableName, pair.SRTableSuffix, originalTableName, columnConfig.Type)
					return columnConfig.Type
				}
			}
		}
	}
	return "bigint"
}

// getDefaultTimestampValue 根据数据类型获取默认的最大时间戳值
func (v *ViewBuilder) getDefaultTimestampValue(dataType string) (string, error) {
	switch strings.ToLower(dataType) {
	case "date":
		return "'9999-12-31'", nil
	case "datetime":
		return "'9999-12-31 23:59:59'", nil
	case "bigint":
		return "9999999999999", nil
	default:
		return "", fmt.Errorf("不支持的时间戳数据类型: %s，仅支持date、datetime、bigint", dataType)
	}
}

// formatTimestampValue 根据数据类型格式化时间戳值
func (v *ViewBuilder) formatTimestampValue(value string, dataType string) (string, error) {
	switch strings.ToLower(dataType) {
	case "date", "datetime":
		// 对于日期时间类型，需要用单引号包围
		if !strings.HasPrefix(value, "'") {
			return fmt.Sprintf("'%s'", value), nil
		}
		return value, nil
	case "bigint":
		// 对于数字类型，直接返回
		return value, nil
	default:
		return "", fmt.Errorf("不支持的时间戳数据类型: %s，仅支持date、datetime、bigint", dataType)
	}
}

// func (v *ViewBuilder) GenViewSQLWithType(ckQ, srQ string, sqlType string) (string, error) {
// 	logger.Debug("开始生成视图SQL (类型: %s)", sqlType)
// 	logger.Debug("ClickHouse查询SQL: %s", ckQ)
// 	logger.Debug("StarRocks查询SQL: %s", srQ)

// 	// 先执行子查询获取固定的时间值
// 	minTimestampQuery := fmt.Sprintf("select min(recordTimestamp) from %s.%s", v.sr.DBName, v.sr.Name)
// 	logger.Debug("执行子查询获取最小时间戳: %s", minTimestampQuery)

// 	// 查询最小时间戳，使用通用的重试wrapper
// 	var nullableTimestamp *int64
// 	var minTimestamp string

func (v *ViewBuilder) GenViewSQLWithType(ckQ, srQ string, sqlType string) (string, error) {
	logger.Debug("开始生成视图SQL (类型: %s)", sqlType)
	logger.Debug("ClickHouse查询SQL: %s", ckQ)
	logger.Debug("StarRocks查询SQL: %s", srQ)

	// 获取时间戳列名和数据类型
	timestampColumn := v.getTimestampColumnName(v.sr.Name)
	timestampType := v.getTimestampColumnType(v.sr.Name)

	// 先执行子查询获取固定的时间值
	minTimestampQuery := fmt.Sprintf("select min(%s) from %s.%s", timestampColumn, v.sr.DBName, v.sr.Name)
	logger.Debug("执行子查询获取最小时间戳: %s", minTimestampQuery)

	// 查询最小时间戳，使用通用的重试wrapper
	var minTimestamp string

	db, err := v.dbManager.GetStarRocksConnection()
	if err != nil {
		return "", fmt.Errorf("获取StarRocks连接失败: %w", err)
	}

	// 根据数据类型选择不同的扫描方式
	switch strings.ToLower(timestampType) {
	case "datetime", "date":
		var nullableTimestamp *string
		retryConfig := retry.Config{
			MaxRetries: v.config.Retry.MaxRetries,
			Delay:      time.Duration(v.config.Retry.DelayMs) * time.Millisecond,
		}
		err = retry.QueryRowAndScanWithRetry(db, retryConfig, minTimestampQuery, []interface{}{&nullableTimestamp})
		if err != nil {
			if err == sql.ErrNoRows {
				logger.Warn("表中没有数据，使用最大默认值")
				minTimestamp, err = v.getDefaultTimestampValue(timestampType)
				if err != nil {
					return "", fmt.Errorf("获取默认时间戳值失败: %w", err)
				}
			} else {
				return "", fmt.Errorf("查询最小时间戳失败: %w", err)
			}
		} else if nullableTimestamp == nil {
			logger.Warn("查询最小时间戳结果为NULL，使用最大默认值")
			minTimestamp, err = v.getDefaultTimestampValue(timestampType)
			if err != nil {
				return "", fmt.Errorf("获取默认时间戳值失败: %w", err)
			}
		} else {
			minTimestamp, err = v.formatTimestampValue(*nullableTimestamp, timestampType)
			if err != nil {
				return "", fmt.Errorf("格式化时间戳值失败: %w", err)
			}
			logger.Debug("获取到最小时间戳: %s", minTimestamp)
		}
	case "bigint":
		var nullableTimestamp *int64
		retryConfig := retry.Config{
			MaxRetries: v.config.Retry.MaxRetries,
			Delay:      time.Duration(v.config.Retry.DelayMs) * time.Millisecond,
		}
		err = retry.QueryRowAndScanWithRetry(db, retryConfig, minTimestampQuery, []interface{}{&nullableTimestamp})
		if err != nil {
			if err == sql.ErrNoRows {
				logger.Warn("表中没有数据，使用最大默认值")
				minTimestamp, err = v.getDefaultTimestampValue(timestampType)
				if err != nil {
					return "", fmt.Errorf("获取默认时间戳值失败: %w", err)
				}
			} else {
				return "", fmt.Errorf("查询最小时间戳失败: %w", err)
			}
		} else if nullableTimestamp == nil {
			logger.Warn("查询最小时间戳结果为NULL，使用最大默认值")
			minTimestamp, err = v.getDefaultTimestampValue(timestampType)
			if err != nil {
				return "", fmt.Errorf("获取默认时间戳值失败: %w", err)
			}
		} else {
			minTimestamp = fmt.Sprintf("%d", *nullableTimestamp)
			logger.Debug("获取到最小时间戳: %s", minTimestamp)
		}
	default:
		return "", fmt.Errorf("未知的时间戳数据类型: %s", timestampType)
	}
	// 统一封装最终SQL拼接
	sql := v.ComposeFinalSQL(sqlType, ckQ, srQ, timestampColumn, minTimestamp)

	logger.Debug("最终视图SQL:\n%s", sql)
	return sql, nil
}

// ComposeFinalSQL 封装最终的 CREATE/ALTER 视图SQL拼接
func (v *ViewBuilder) ComposeFinalSQL(sqlType, ckQ, srQ, timestampColumn, minTimestamp string) string {
	body := fmt.Sprintf("%s.%s as \n%s \nwhere %s < %s \nunion all \n%s \nwhere %s >= %s; \n",
		v.dbName, v.viewName, ckQ, timestampColumn, minTimestamp, srQ, timestampColumn, minTimestamp)
	if sqlType == SQLTypeAlter {
		return "alter view " + body
	}
	return "create view if not exists " + body
}

// 构建clause
func (f *CKField) GenClause() {
	// 开始构建
	if ckc.IsArrayIPV6(f.OriginType()) {
		f.Clause = f.ArrayIPV6()
	} else if ckc.IsStringArray(f.OriginType()) {
		f.Clause = f.Array()
	} else if ckc.IsArray(f.OriginType()) {
		f.Clause = f.ArrayMap()
	} else if f.IsAddedColumn() {
		f.Clause = fmt.Sprintf("`%s` as `%s`", f.Field.Name, f.SRField.Name)
	} else {
		f.Clause = fmt.Sprintf("`%s`", f.Field.Name)
	}
}

func (f *CKField) ArrayMap() string {
	return fmt.Sprintf("CASE \n\t\tWHEN `%s` = '' THEN %s[]\n\t\tELSE array_map(x -> CAST(x AS %s), split(`%s`, 'CKTOSRFRAGEMENT'))\n\tEND as `%s`", f.Field.Name, f.SRField.Type, f.SRBasicType(), f.Field.Name, f.SRField.Name)
}

func (f *CKField) Array() string {
	return fmt.Sprintf("CASE \n\t\tWHEN `%s` = '' THEN ARRAY<String>[]\n\t\tELSE split(`%s`, 'CKTOSRFRAGEMENT')\n\tEND as `%s`", f.Field.Name, f.Field.Name, f.SRField.Name)
}

func (f *CKField) ArrayIPV6() string {
	return fmt.Sprintf("CASE \n\t\tWHEN `%s` = '' THEN ARRAY<LARGEINT>[]\n\t\tELSE array_map(x -> CAST(x AS LARGEINT), split(`%s`, 'CKTOSRFRAGEMENT'))\n\tEND as `%s`", f.Field.Name, f.Field.Name, f.SRField.Name)
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
func (v *ViewBuilder) MapSRField(field ckc.FieldConverter, srNameFieldMap map[string]SRField) (SRField, error) {
	name := field.OriginName()
	if ckc.IsArrayIPV6(field.OriginType()) || ckc.IsArrayIPV4(field.OriginType()) {
		name = fmt.Sprintf("%s_int", field.OriginName())
	} else if ckc.IsIPV6(field.OriginType()) || ckc.IsIPV4(field.OriginType()) {
		name = fmt.Sprintf("%s_int", field.OriginName())
	}

	if v, ok := srNameFieldMap[name]; ok {
		return v, nil
	} else {
		return SRField{}, fmt.Errorf("map failed, column %s not exists in sr", name)
	}
}
