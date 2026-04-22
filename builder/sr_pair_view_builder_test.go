package builder

import (
	"strings"
	"testing"

	"cksr/parser"
)

func TestSRPairViewBuilderBuild(t *testing.T) {
	oldTable := parser.Table{
		DDL: parser.DDL{TableName: "asset_old"},
		Field: []parser.Field{
			{Name: "id", Type: "bigint"},
			{Name: "event_time", Type: "datetime"},
		},
	}
	newTable := parser.Table{
		DDL: parser.DDL{TableName: "asset_new"},
		Field: []parser.Field{
			{Name: "id", Type: "bigint"},
			{Name: "event_time", Type: "datetime"},
			{Name: "event_day", Type: "date", DefaultKind: "AS", DefaultExpr: "(date_trunc('day', event_time))"},
			{Name: "tenant", Type: "varchar(20)", DefaultKind: "DEFAULT", DefaultExpr: "'default_tenant'"},
		},
	}

	sql, err := NewSRPairViewBuilder("business", "asset", oldTable, newTable).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	wants := []string{
		"CREATE VIEW `business`.`asset` AS",
		"FROM `business`.`asset_old`",
		"FROM `business`.`asset_new`",
		"CAST((date_trunc('day', event_time)) AS date) AS `event_day`",
		"CAST('default_tenant' AS varchar(20)) AS `tenant`",
	}
	for _, want := range wants {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}
}

func TestSRPairViewBuilderBuildFailsWhenNewOnlyColumnCannotBeFilled(t *testing.T) {
	oldTable := parser.Table{
		DDL: parser.DDL{TableName: "asset_old"},
		Field: []parser.Field{
			{Name: "id", Type: "bigint"},
		},
	}
	newTable := parser.Table{
		DDL: parser.DDL{TableName: "asset_new"},
		Field: []parser.Field{
			{Name: "id", Type: "bigint"},
			{Name: "tenant", Type: "varchar(20)"},
		},
	}

	_, err := NewSRPairViewBuilder("business", "asset", oldTable, newTable).Build()
	if err == nil {
		t.Fatal("expected Build to fail for unsupported new-only column")
	}
	if !strings.Contains(err.Error(), "tenant") {
		t.Fatalf("expected error to mention tenant, got %v", err)
	}
}
