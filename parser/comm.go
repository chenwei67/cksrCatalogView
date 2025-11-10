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
	TTL       string
}

type Field struct {
    Name           string
    Type           string
    // 仅保留默认值相关
    DefaultKind    string // CK: DEFAULT/MATERIALIZED/EPHEMERAL/ALIAS；SR: DEFAULT/AS
    DefaultExpr    string // 默认值或生成表达式（原文）
}



type Table struct {
	DDL              DDL
	Field            []Field
}
