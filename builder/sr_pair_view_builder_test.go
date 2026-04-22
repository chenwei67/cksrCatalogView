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

func TestSRPairViewBuilderBuildWithTimeBoundary(t *testing.T) {
	oldTable := parser.Table{
		DDL: parser.DDL{TableName: "asset_old"},
		Field: []parser.Field{
			{Name: "id", Type: "bigint"},
			{Name: "recordTimestamp", Type: "bigint"},
		},
	}
	newTable := parser.Table{
		DDL: parser.DDL{TableName: "asset_new"},
		Field: []parser.Field{
			{Name: "id", Type: "bigint"},
			{Name: "recordTimestamp", Type: "bigint"},
			{Name: "tenant", Type: "varchar(20)", DefaultKind: "DEFAULT", DefaultExpr: "'default_tenant'"},
		},
	}

	sql, err := NewSRPairViewBuilder("business", "asset", oldTable, newTable).BuildWithTimeBoundary("recordTimestamp", "1735689600")
	if err != nil {
		t.Fatalf("BuildWithTimeBoundary returned error: %v", err)
	}

	wants := []string{
		"WHERE `recordTimestamp` < 1735689600",
	}
	for _, want := range wants {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "WHERE `recordTimestamp` >=") {
		t.Fatalf("expected new table query to have no timestamp filter, got:\n%s", sql)
	}
}

func TestSRPairViewBuilderBuildAllowsSameColumns(t *testing.T) {
	oldTable := parser.Table{
		DDL: parser.DDL{TableName: "asset_old"},
		Field: []parser.Field{
			{Name: "id", Type: "bigint"},
			{Name: "recordTimestamp", Type: "bigint"},
		},
	}
	newTable := parser.Table{
		DDL: parser.DDL{TableName: "asset_new"},
		Field: []parser.Field{
			{Name: "id", Type: "bigint"},
			{Name: "recordTimestamp", Type: "bigint"},
		},
	}

	sql, err := NewSRPairViewBuilder("business", "asset", oldTable, newTable).Build()
	if err != nil {
		t.Fatalf("expected same-column tables to be allowed, got error: %v", err)
	}
	if !strings.Contains(sql, "FROM `business`.`asset_old`") || !strings.Contains(sql, "FROM `business`.`asset_new`") {
		t.Fatalf("unexpected sql for same-column tables:\n%s", sql)
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

func TestSRPairViewBuilderBuildUsesNullForNullableNewColumn(t *testing.T) {
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
			{Name: "remark", Type: "varchar(64)", IsNullable: true},
		},
	}

	sql, err := NewSRPairViewBuilder("business", "asset", oldTable, newTable).Build()
	if err != nil {
		t.Fatalf("expected nullable new column to be filled by NULL, got error: %v", err)
	}
	if !strings.Contains(sql, "CAST(NULL AS varchar(64)) AS `remark`") {
		t.Fatalf("expected SQL to contain NULL fill clause, got:\n%s", sql)
	}
}
