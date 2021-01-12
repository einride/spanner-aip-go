package descriptorcodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestDatabaseDescriptorCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "database", func(database *spanddl.Database, f *codegen.File) {
		DatabaseDescriptorCodeGenerator{Database: database}.GenerateCode(f)
	})
}
