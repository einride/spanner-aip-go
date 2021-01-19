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
		RowCodeGenerator{Table: table}.GenerateCode(f)
	}
	for _, table := range g.Database.Tables {
		KeyCodeGenerator{Table: table}.GenerateCode(f)
	}
	for _, table := range g.Database.Tables {
		RowIteratorCodeGenerator{Table: table}.GenerateCode(f)
	}
	ReadTransactionCodeGenerator(g).GenerateCode(f)
	CommonCodeGenerator{}.GenerateCode(f)
}
