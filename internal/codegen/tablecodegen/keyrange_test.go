package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestKeyRangeCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "keyrange", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			KeyRangeCodeGenerator{Table: table}.GenerateCode(f)
			PartialKeyCodeGenerator{Table: table}.GenerateCode(f)
		}
	})
}
