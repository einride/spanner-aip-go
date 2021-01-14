package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestRowCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "row", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			RowCodeGenerator{Table: table}.GenerateCode(f)
			KeyCodeGenerator{Table: table}.GenerateCode(f)
		}
	})
}
