package databasecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestDatabaseCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "database", func(db *spanddl.Database, f *codegen.File) {
		DatabaseCodeGenerator{Database: db}.GenerateCode(f)
	})
}
