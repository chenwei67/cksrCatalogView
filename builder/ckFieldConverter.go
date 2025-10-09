package builder

import (
	"fmt"
	"strconv"
	"strings"

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
	realType := reformat(f.Type)
	if realType == "" {
		return FieldConverter{}, fmt.Errorf("field %s type %s invalid", f.Name, f.Type)
	}
	// 类型检查
	err := typeCheck(realType)
	if err != nil {
		return FieldConverter{}, err
	}

	f.Type = realType
	fp := &f
	newF := ckNameMap(fp, names)
	if newF == fp {
		return FieldConverter{
			Field:       fp,
			OriginField: nil,
		}, nil
	}
	return FieldConverter{
		Field:       newF,
		OriginField: fp,
	}, nil
}

func NewConverters(table parser.Table) ([]FieldConverter, error) {
	var res []FieldConverter
	names := make(map[string]struct{})
	for _, f := range table.Field {
		names[f.Name] = struct{}{}
	}

	for _, f := range table.Field {
		// 忽略物化字段
		if f.IsMaterialized {
			continue
		}
		
		// 过滤掉通过add column操作新增的字段
		if IsAddedColumnByName(f.Name) {
			continue
		}
		
		c, err := NewFieldConverter(f, names)
		if err != nil {
			return nil, fmt.Errorf("build FieldConverter for %s of table %s.%s failed, %s", f.Name, table.DDL.DBName, table.DDL.TableName, err.Error())
		}
		res = append(res, c)
	}
	return res, nil
}

func ckNameMap(f *parser.Field, names map[string]struct{}) *parser.Field {
	newF := new(parser.Field)
	if IsArray(f.Type) {
		newF.Name = nameWithSuffix(f.Name, names)
		newF.Type = TypeString
		return newF
	}
	if IsIPV4(f.Type) {
		newF.Name = nameWithSuffix(f.Name, names)
		newF.Type = TypeUInt32
		return newF
	}
	if IsIPV6(f.Type) {
		newF.Name = nameWithSuffix(f.Name, names)
		newF.Type = TypeUInt128
		return newF
	}
	return f
}

func nameWithSuffix(name string, names map[string]struct{}) string {
	name = fmt.Sprintf("%s%s", name, nameSuffix)
	for i := 0; ; i++ {
		if _, ok := names[name]; !ok {
			return name
		}
		name = fmt.Sprintf("%s%d", name, i)
		names[name] = struct{}{}
	}
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
				fmt.Printf("t is %s, err: %s \n", t, err.Error())
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
