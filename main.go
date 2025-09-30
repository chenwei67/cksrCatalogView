package main

import (
	"fmt"
	"os"

	"cksr/builder"
	"cksr/parser"
)

func main() {
	alterSql, viewSql, err := networkSecurityLog("test")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	fmt.Println(alterSql)
	fmt.Println(viewSql)
}

func getParseTable(sqlPath string) (parser.Table, error) {
	s, err := os.ReadFile(sqlPath)
	if err != nil {
		return parser.Table{}, fmt.Errorf("read file failed: %s", err.Error())
	}
	t := parser.ParserTableSQL(string(s))
	return t, nil
}

func networkSecurityLog(catalogName string) (string, string, error) {
	ckTable, err := getParseTable("D:\\Users\\User\\GolandProjects\\srsql\\exportck\\local_hot\\network_security_log_local.sql")
	if err != nil {
		return "", "", err
	}
	srTable, err := getParseTable("D:\\Users\\User\\GolandProjects\\srsql\\sqlchange\\output\\hot\\network_security_log.sql")
	if err != nil {
		return "", "", err
	}
	//for _, f := range srTable.Field {
	//	fmt.Printf("name: %s, type: %s \n", f.Name, f.Type)
	//	if f.Name == "" || f.Type == "" {
	//		fmt.Println("empty!!!!!!!!!!!!!!!!!!!!!")
	//	}
	//}
	alterSql, viewSql, err := run(ckTable, srTable, catalogName)
	if err != nil {
		return "", "", err
	}
	return alterSql, viewSql, nil
}

func run(ckTable, srTable parser.Table, catalogName string) (string, string, error) {
	converters, err := builder.NewConverters(ckTable)
	if err != nil {
		return "", "", err
	}
	alterSql := builder.NewCKAddColumnsBuilder(converters, ckTable.DDL.DBName, ckTable.DDL.TableName).Build()
	viewBuilder := builder.NewBuilder(converters, srTable.Field, ckTable.DDL.DBName, ckTable.DDL.TableName, catalogName, srTable.DDL.DBName, srTable.DDL.TableName)
	view, err := viewBuilder.Build()
	if err != nil {
		return "", "", err
	}
	return alterSql, view, nil
}
