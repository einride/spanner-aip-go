package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestRowIteratorCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "rowiterator", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			RowIteratorCodeGenerator{Table: table}.GenerateCode(f)
			RowCodeGenerator{Table: table}.GenerateCode(f)
			PrimaryKeyCodeGenerator{Table: table}.GenerateCode(f)
		}
	})
}
