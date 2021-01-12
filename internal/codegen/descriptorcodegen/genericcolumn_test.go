package descriptorcodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestGenericColumnDescriptorCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "genericcolumn", func(db *spanddl.Database, f *codegen.File) {
		GenericColumnDescriptorCodeGenerator{}.GenerateCode(f)
	})
}
