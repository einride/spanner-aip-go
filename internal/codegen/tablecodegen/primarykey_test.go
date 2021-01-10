package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestPrimaryKeyCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "primarykey", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			PrimaryKeyCodeGenerator{Table: table}.GenerateCode(f)
		}
	})
}
