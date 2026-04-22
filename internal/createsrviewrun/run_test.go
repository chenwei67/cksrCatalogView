package createsrviewrun

import (
	"os"
	"path/filepath"
	"testing"
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
