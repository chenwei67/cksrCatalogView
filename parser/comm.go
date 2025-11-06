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
}



type Table struct {
	DDL              DDL
	Field            []Field
}
