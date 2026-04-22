package parser

import "testing"

func TestParserTableSQLParsesDefaultAndGeneratedColumns(t *testing.T) {
	ddl := "CREATE TABLE `test_db`.`asset_new` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `event_time` datetime DEFAULT CURRENT_TIMESTAMP,\n" +
		"  `event_day` date AS (date_trunc('day', event_time))\n" +
		") ENGINE=OLAP\n"

	table := ParserTableSQL(ddl)
	if len(table.Field) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(table.Field))
	}

	if table.Field[1].DefaultKind != "DEFAULT" {
		t.Fatalf("expected DEFAULT kind, got %q", table.Field[1].DefaultKind)
	}
	if table.Field[1].DefaultExpr != "CURRENT_TIMESTAMP" {
		t.Fatalf("expected default expr CURRENT_TIMESTAMP, got %q", table.Field[1].DefaultExpr)
	}

	if table.Field[2].DefaultKind != "AS" {
		t.Fatalf("expected AS kind, got %q", table.Field[2].DefaultKind)
	}
	if table.Field[2].DefaultExpr != "(date_trunc('day', event_time))" {
		t.Fatalf("unexpected generated expr: %q", table.Field[2].DefaultExpr)
	}
}
