package builder

import (
	"fmt"
	"strconv"
	"strings"

	"cksr/logger"
	"cksr/parser"
)

const (
	nameSuffix  = "_TmpSrConverter"
	TypeString  = "String"
	TypeUInt32  = "UInt32"
	TypeUInt128 = "UInt128"
)

type FieldConverter struct {
	*parser.Field
	OriginField *parser.Field
}

func (f FieldConverter) originName() string {
	name := f.Name
	if f.OriginField != nil {
		name = f.OriginField.Name
	}
	return name
}

func (f FieldConverter) originType() string {
	t := f.Type
	if f.OriginField != nil {
		t = f.OriginField.Type
	}
	return t
}

// IsAddedColumn 检查字段是否是通过add column操作新增的字段
func (f FieldConverter) IsAddedColumn() bool {
	// 检查字段名是否包含add column操作的后缀
	return strings.Contains(f.Name, nameSuffix)
}

// IsAddedColumnByName 检查给定的字段名是否是通过add column操作新增的字段
func IsAddedColumnByName(fieldName string) bool {
	return strings.Contains(fieldName, nameSuffix)
}

// 对数据类型进行重新解析
// 如果不支持这种数据类型，就报错
// 如果支持，构造好当前的fieldConverter，内嵌的Field只需要type和name
func NewFieldConverter(f parser.Field, names map[string]struct{}) (FieldConverter, error) {
	logger.Debug("NewFieldConverter开始处理字段 %s，原始类型: %s", f.Name, f.Type)

	realType := reformat(f.Type)
	if realType == "" {
		logger.Error("字段类型无效 - 字段: %s, 类型: %s", f.Name, f.Type)
		return FieldConverter{}, fmt.Errorf("field %s type %s invalid", f.Name, f.Type)
	}
	logger.Debug("字段 %s 类型重新格式化: %s -> %s", f.Name, f.Type, realType)

	// 类型检查
	err := typeCheck(realType)
	if err != nil {
		logger.Error("字段类型检查失败 - 字段: %s, 类型: %s, 错误: %v", f.Name, realType, err)
		return FieldConverter{}, err
	}
	logger.Debug("字段 %s 类型检查通过", f.Name)

	f.Type = realType
	fp := &f
	newF := ckNameMap(fp, names)
	if newF == fp {
		logger.Debug("字段 %s 无需名称映射", f.Name)
		return FieldConverter{
			Field:       fp,
			OriginField: nil,
		}, nil
	}
	logger.Debug("字段 %s 需要名称映射: %s -> %s", f.Name, fp.Name, newF.Name)
	return FieldConverter{
		Field:       newF,
		OriginField: fp,
	}, nil
}

func NewConverters(table parser.Table) ([]FieldConverter, error) {
	logger.Debug("NewConverters开始处理表 %s.%s，总字段数: %d", table.DDL.DBName, table.DDL.TableName, len(table.Field))

	var res []FieldConverter
	names := make(map[string]struct{})
	for _, f := range table.Field {
		names[f.Name] = struct{}{}
	}

	processedCount := 0
	skippedMaterialized := 0
	skippedAdded := 0

	for i, f := range table.Field {
		if i > 0 && i%50 == 0 {
			logger.Debug("NewConverters进度: 已处理 %d/%d 字段", i, len(table.Field))
		}

		// 忽略物化字段
		if f.IsMaterialized {
			skippedMaterialized++
			continue
		}

		// 过滤掉通过add column操作新增的字段
		if IsAddedColumnByName(f.Name) {
			skippedAdded++
			continue
		}

		logger.Debug("正在处理字段 %s (类型: %s)", f.Name, f.Type)
		c, err := NewFieldConverter(f, names)
		if err != nil {
			logger.Error("创建字段转换器失败 - 字段: %s, 错误: %v", f.Name, err)
			return nil, fmt.Errorf("build FieldConverter for %s of table %s.%s failed, %s", f.Name, table.DDL.DBName, table.DDL.TableName, err.Error())
		}
		res = append(res, c)
		processedCount++
	}

	logger.Debug("NewConverters完成 - 总字段: %d, 处理: %d, 跳过物化: %d, 跳过新增: %d",
		len(table.Field), processedCount, skippedMaterialized, skippedAdded)

	return res, nil
}

func ckNameMap(f *parser.Field, names map[string]struct{}) *parser.Field {
	logger.Debug("ckNameMap开始处理字段: %s (类型: %s)", f.Name, f.Type)

	newF := new(parser.Field)
	if IsArrayIPV6(f.Type) {
		logger.Debug("字段 %s 是Array(IPv6)类型，需要名称映射", f.Name)
		newF.Name = nameWithSuffix(f.Name, names)
		newF.Type = TypeString
		logger.Debug("Array(IPv6)字段映射完成: %s -> %s (类型: %s)", f.Name, newF.Name, newF.Type)
		return newF
	} else if IsArray(f.Type) {
		logger.Debug("字段 %s 是数组类型，需要名称映射", f.Name)
		newF.Name = nameWithSuffix(f.Name, names)
		newF.Type = TypeString
		logger.Debug("数组字段映射完成: %s -> %s (类型: %s)", f.Name, newF.Name, newF.Type)
		return newF
	}
	if IsIPV4(f.Type) {
		logger.Debug("字段 %s 是IPv4类型，需要名称映射", f.Name)
		newF.Name = nameWithSuffix(f.Name, names)
		newF.Type = TypeUInt32
		logger.Debug("IPv4字段映射完成: %s -> %s (类型: %s)", f.Name, newF.Name, newF.Type)
		return newF
	}
	if IsIPV6(f.Type) {
		logger.Debug("字段 %s 是IPv6类型，需要名称映射", f.Name)
		newF.Name = nameWithSuffix(f.Name, names)
		newF.Type = TypeUInt128
		logger.Debug("IPv6字段映射完成: %s -> %s (类型: %s)", f.Name, newF.Name, newF.Type)
		return newF
	}
	logger.Debug("字段 %s 无需特殊映射，直接返回", f.Name)
	return f
}

func nameWithSuffix(name string, names map[string]struct{}) string {
	logger.Debug("nameWithSuffix开始处理字段名: %s", name)

	newName := fmt.Sprintf("%s%s", name, nameSuffix)
	logger.Debug("生成带后缀的名称: %s", newName)

	// 检查基础名称是否已存在
	if _, ok := names[newName]; !ok {
		logger.Debug("名称 %s 可用，直接返回", newName)
		return newName
	}

	// 如果已经存在带TmpSrConverter后缀的列，直接使用该列
	logger.Debug("发现已存在带TmpSrConverter后缀的列: %s，直接复用", newName)
	return newName
}

// 括号匹配最内层数据，获取真实数据类型
func reformat(t string) string {
	var (
		w []byte
	)
	// 如果是Array，需要特殊处理
	for i := 0; i < len(t); i++ {
		if t[i] == '(' {
			if string(w) != "Array" {
				// 直接断路掉，不支持嵌套array
				if strings.HasPrefix(string(w), "Array(") {
					return ""
				}
				w = []byte{}
			} else {
				w = append(w, t[i])
			}
		} else if t[i] == ')' {
			if string(w[:5]) == "Array" {
				w = append(w, t[i])
			}
			return string(w)
		} else {
			w = append(w, t[i])
		}
	}
	return t
}

var NotSupportTypeErr = fmt.Errorf("not support type")

func typeCheck(t string) error {
	integerTypes := []string{"Int", "UInt"}
	for _, integerType := range integerTypes {
		if strings.HasPrefix(t, integerType) {
			remain := t[len(integerType):]
			if remain == "" {
				return nil
			}
			_, err := strconv.Atoi(remain)
			if err != nil {
				logger.Error("类型解析错误 - t: %s, err: %s", t, err.Error())
				return fmt.Errorf("%w %s", NotSupportTypeErr, t)
			}
			return nil
		}
	}

	if IsArray(t) {
		return nil
	}
	var supportFieldSet = []string{
		"Float32",
		"Float64",
		"String",
		"DateTime",
		"IPv4",
		"IPv6",
		"Bool",
	}
	supportTypeMap := make(map[string]bool)
	for _, t := range supportFieldSet {
		supportTypeMap[t] = true
	}
	if supportTypeMap[t] {
		return nil
	}
	return fmt.Errorf("%w %s", NotSupportTypeErr, t)
}

func IsStringArray(t string) bool {
	return strings.ToLower(t) == "array(string)"
}

func IsArray(t string) bool {
	return strings.HasPrefix(strings.ToLower(t), "array(")
}

func IsIPV4(t string) bool {
	return strings.ToLower(t) == "ipv4"
}

func IsIPV6(t string) bool {
	return strings.ToLower(t) == "ipv6"
}

func IsArrayIPV6(t string) bool {
	return strings.ToLower(t) == "array(ipv6)"
}

func IsArrayIPV4(t string) bool {
	return strings.ToLower(t) == "array(ipv4)"
}
