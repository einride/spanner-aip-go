package databasecodegen

import (
	"testing"

	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

func TestDatabaseCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "database", func(db *spanddl.Database, f *codegen.File) {
		DatabaseCodeGenerator{Database: db}.GenerateCode(f)
	})
}
