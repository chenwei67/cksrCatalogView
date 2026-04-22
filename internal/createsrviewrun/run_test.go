package createsrviewrun

import (
	"os"
	"path/filepath"
	"testing"

	"cksr/config"
)

func TestWriteGeneratedSQLFiles(t *testing.T) {
	outputDir := t.TempDir()
	generatedViews := []GeneratedViewSQL{
		{
			BaseName: "asset",
			SQL:      "CREATE VIEW `business`.`asset` AS SELECT 1;\n",
		},
		{
			BaseName: "alarm",
			SQL:      "CREATE VIEW `business`.`alarm` AS SELECT 2;\n",
		},
	}

	if err := writeGeneratedSQLFiles(outputDir, generatedViews); err != nil {
		t.Fatalf("writeGeneratedSQLFiles returned error: %v", err)
	}

	for _, generatedView := range generatedViews {
		filePath := filepath.Join(outputDir, generatedView.BaseName+".sql")
		content, err := os.ReadFile(filePath)
		if err != nil {
			t.Fatalf("read generated sql file failed: %v", err)
		}
		if string(content) != generatedView.SQL {
			t.Fatalf("unexpected sql content for %s: %q", generatedView.BaseName, string(content))
		}
	}
}

func TestTimestampConfigHelpers(t *testing.T) {
	cfg := &config.Config{
		DatabasePairs: []config.DatabasePair{
			{SRTableSuffix: "_new"},
		},
		TimestampColumns: map[string]config.TimestampColumnConfig{
			"asset": {
				Column: "event_time",
				Type:   "datetime",
			},
		},
	}

	if got := getTimestampColumnName(cfg, "asset_new"); got != "event_time" {
		t.Fatalf("unexpected timestamp column: %s", got)
	}
	if got := getTimestampColumnType(cfg, "asset_new"); got != "datetime" {
		t.Fatalf("unexpected timestamp type: %s", got)
	}
	if got := getTimestampColumnName(cfg, "unknown"); got != "recordTimestamp" {
		t.Fatalf("unexpected default timestamp column: %s", got)
	}
	if got := getTimestampColumnType(cfg, "unknown"); got != "bigint" {
		t.Fatalf("unexpected default timestamp type: %s", got)
	}
}

func TestTimestampFormatHelpers(t *testing.T) {
	got, err := formatTimestampValue("2025-01-01 00:00:00", "datetime")
	if err != nil || got != "'2025-01-01 00:00:00'" {
		t.Fatalf("unexpected datetime format result: got=%s err=%v", got, err)
	}
	got, err = getDefaultTimestampValue("bigint")
	if err != nil || got != "9999999999999" {
		t.Fatalf("unexpected bigint default result: got=%s err=%v", got, err)
	}
}
