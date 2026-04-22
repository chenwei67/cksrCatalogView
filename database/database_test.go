package database

import (
	"database/sql"
	"testing"
)

func TestBuildStarRocksTableSchema(t *testing.T) {
	table, err := buildStarRocksTableSchema("business", "asset_new", []StarRocksColumnSchema{
		{
			ColumnName:      "tenant",
			OrdinalPosition: 4,
			ColumnType:      "varchar(20)",
			IsNullable:      "NO",
			ColumnDefault:   sql.NullString{String: "'default_tenant'", Valid: true},
		},
		{
			ColumnName:           "event_day",
			OrdinalPosition:      3,
			ColumnType:           "date",
			IsNullable:           "NO",
			GenerationExpression: sql.NullString{String: "date_trunc('day', event_time)", Valid: true},
		},
		{
			ColumnName:      "id",
			OrdinalPosition: 1,
			ColumnType:      "bigint",
			IsNullable:      "NO",
		},
		{
			ColumnName:      "event_time",
			OrdinalPosition: 2,
			ColumnType:      "datetime",
			IsNullable:      "YES",
		},
	})
	if err != nil {
		t.Fatalf("buildStarRocksTableSchema returned error: %v", err)
	}

	if table.DDL.DBName != "business" || table.DDL.TableName != "asset_new" {
		t.Fatalf("unexpected ddl info: %+v", table.DDL)
	}
	if len(table.Field) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(table.Field))
	}
	if table.Field[2].Name != "event_day" || table.Field[2].DefaultKind != "AS" || table.Field[2].DefaultExpr != "date_trunc('day', event_time)" {
		t.Fatalf("unexpected generated field: %+v", table.Field[2])
	}
	if table.Field[3].Name != "tenant" || table.Field[3].DefaultKind != "DEFAULT" || table.Field[3].DefaultExpr != "'default_tenant'" {
		t.Fatalf("unexpected default field: %+v", table.Field[3])
	}
}

func TestBuildStarRocksTableSchemaRejectsEmptyColumns(t *testing.T) {
	_, err := buildStarRocksTableSchema("business", "asset_new", nil)
	if err == nil {
		t.Fatal("expected error for empty columns")
	}
}

func TestBuildStarRocksTableSchemaHandlesEmptyStringDefault(t *testing.T) {
	table, err := buildStarRocksTableSchema("business", "asset_new", []StarRocksColumnSchema{
		{
			ColumnName:      "manage",
			OrdinalPosition: 1,
			ColumnType:      "varchar(65533)",
			IsNullable:      "YES",
			ColumnDefault:   sql.NullString{String: "\"\"", Valid: true},
		},
	})
	if err != nil {
		t.Fatalf("buildStarRocksTableSchema returned error: %v", err)
	}
	if len(table.Field) != 1 {
		t.Fatalf("expected 1 field, got %d", len(table.Field))
	}
	if table.Field[0].DefaultKind != "DEFAULT" || table.Field[0].DefaultExpr != "''" {
		t.Fatalf("unexpected empty string default mapping: %+v", table.Field[0])
	}
}

func TestBuildStarRocksTableSchemaHandlesNullableColumn(t *testing.T) {
	table, err := buildStarRocksTableSchema("business", "asset_new", []StarRocksColumnSchema{
		{
			ColumnName:      "remark",
			OrdinalPosition: 1,
			ColumnType:      "varchar(64)",
			IsNullable:      "YES",
		},
	})
	if err != nil {
		t.Fatalf("buildStarRocksTableSchema returned error: %v", err)
	}
	if !table.Field[0].IsNullable {
		t.Fatalf("expected field to be nullable: %+v", table.Field[0])
	}
	if table.Field[0].DefaultKind != "" || table.Field[0].DefaultExpr != "" {
		t.Fatalf("nullable field without default should not synthesize default metadata: %+v", table.Field[0])
	}
}
