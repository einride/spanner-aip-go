package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestReadTransactionCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "readtransaction", func(db *spanddl.Database, f *codegen.File) {
		for _, table := range db.Tables {
			ReadTransactionCodeGenerator{Table: table}.GenerateCode(f)
			RowIteratorCodeGenerator{Table: table}.GenerateCode(f)
			RowCodeGenerator{Table: table}.GenerateCode(f)
			PrimaryKeyCodeGenerator{Table: table}.GenerateCode(f)
		}
		CommonCodeGenerator{}.GenerateCode(f)
	})
}
