package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestInterleavedRowIteratorCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "interleavedrowiterator", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			if interleavedTables := db.InterleavedTables(table.Name); len(interleavedTables) > 0 {
				InterleavedRowIteratorCodeGenerator{
					Table:             table,
					InterleavedTables: interleavedTables,
				}.GenerateCode(f)
				InterleavedRowCodeGenerator{
					Table:             table,
					InterleavedTables: interleavedTables,
				}.GenerateCode(f)
			}
			RowCodeGenerator{Table: table}.GenerateCode(f)
			PrimaryKeyCodeGenerator{Table: table}.GenerateCode(f)
		}
		CommonCodeGenerator{}.GenerateCode(f)
	})
}
