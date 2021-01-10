package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestPartialKeyCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "partialkey", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			PartialKeyCodeGenerator{Table: table}.GenerateCode(f)
		}
	})
}
