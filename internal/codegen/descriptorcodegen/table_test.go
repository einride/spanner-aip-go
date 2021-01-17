package descriptorcodegen

import (
	"testing"

	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

func TestTableDescriptorCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "table", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			TableDescriptorCodeGenerator{Table: table}.GenerateCode(f)
		}
		GenericColumnDescriptorCodeGenerator{}.GenerateCode(f)
	})
}
