package databasecodegen

import (
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
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
		interleavedTables := g.Database.InterleavedTables(table.Name)
		if len(interleavedTables) == 0 {
			continue
		}
		InterleavedReadTransactionCodeGenerator{
			Table:             table,
			InterleavedTables: interleavedTables,
		}.GenerateCode(f)
		InterleavedRowIteratorCodeGenerator{
			Table:             table,
			InterleavedTables: interleavedTables,
		}.GenerateCode(f)
		InterleavedRowCodeGenerator{
			Table:             table,
			InterleavedTables: interleavedTables,
		}.GenerateCode(f)
	}
	CommonCodeGenerator{}.GenerateCode(f)
}
