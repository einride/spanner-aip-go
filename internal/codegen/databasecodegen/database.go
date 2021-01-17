package databasecodegen

import (
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

type DatabaseCodeGenerator struct {
	Database *spanddl.Database
}

func (g DatabaseCodeGenerator) GenerateCode(f *codegen.File) {
	for _, table := range g.Database.Tables {
		ReadTransactionCodeGenerator{Table: table}.GenerateCode(f)
		RowIteratorCodeGenerator{Table: table}.GenerateCode(f)
		KeyCodeGenerator{Table: table}.GenerateCode(f)
		RowCodeGenerator{Table: table}.GenerateCode(f)
		if len(table.InterleavedTables) == 0 {
			continue
		}
		ParentReadTransactionCodeGenerator{
			Table: table,
		}.GenerateCode(f)
		ParentRowIteratorCodeGenerator{
			Table: table,
		}.GenerateCode(f)
		ParentRowCodeGenerator{
			Table: table,
		}.GenerateCode(f)
	}
	CommonCodeGenerator{}.GenerateCode(f)
}
