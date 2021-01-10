package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestInterleavedRowCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "interleavedrow", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			if interleavedTables := db.InterleavedTables(table.Name); len(interleavedTables) > 0 {
				InterleavedRowCodeGenerator{
					Table:             table,
					InterleavedTables: interleavedTables,
				}.GenerateCode(f)
			}
			RowCodeGenerator{Table: table}.GenerateCode(f)
			PrimaryKeyCodeGenerator{Table: table}.GenerateCode(f)
		}
	})
}
