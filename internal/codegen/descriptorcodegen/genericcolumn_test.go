package descriptorcodegen

import (
	"testing"

	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

func TestGenericColumnDescriptorCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "genericcolumn", func(_ *spanddl.Database, f *codegen.File) {
		GenericColumnDescriptorCodeGenerator{}.GenerateCode(f)
	})
}
