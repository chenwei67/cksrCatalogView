/*
 * @File : parser
 * @Date : 2025/7/9 14:11
 * @Author : Tangshiyu tshiyuoo@gmail.com
 * @Version: 1.0.0
 * @Description:
 */

package parser

type DDL struct {
	TableName string
	DBName    string
	Engine    string
	Partition string
	OrderBy   []string
	TTL       string
}

type Field struct {
	Name           string
	Type           string
	Default        *string
	Comment        *string
	IsAlias        bool
	IsMaterialized bool
}



type Index struct {
	Name   string
	Fields []string
	Type   string
	IF     bool
}

type Table struct {
	DDL              DDL
	Field            []Field
	Index            []Index
	FullSearchFields map[string]string
}
