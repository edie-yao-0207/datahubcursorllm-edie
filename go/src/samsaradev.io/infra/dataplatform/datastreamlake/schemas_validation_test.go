package datastreamlake

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// TestNoUnsupportedStructTypesInSchemas scans datastreamlake SQL schemas and fails
// if any column type uses STRUCT<...>. Our Firehose/Glue ingestion path does not
// support declaring struct-typed columns directly in schemas. Use array/map types
// modeled via Go slices/maps of structs instead, which generate supported types.
func TestNoUnsupportedStructTypesInSchemas(t *testing.T) {
	// Locate the schemas directory relative to this test file.
	// This test file lives in go/src/samsaradev.io/infra/dataplatform/datastreamlake/.
	schemasDir := filepath.Join(".", "schemas")

	entries, err := os.ReadDir(schemasDir)
	if err != nil {
		t.Fatalf("unable to read schemas directory %s: %v", schemasDir, err)
	}

	structPattern := regexp.MustCompile(`(?i)\bstruct\s*<`)
	var violations []string

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".sql" {
			continue
		}
		path := filepath.Join(schemasDir, e.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("unable to read schema file %s: %v", path, err)
		}
		if structPattern.Find(content) != nil {
			violations = append(violations, path)
		}
	}

	if len(violations) > 0 {
		t.Fatalf("Unsupported STRUCT<...> type detected in schema files (use slices/maps of structs in Go Record instead): %v", violations)
	}
}


