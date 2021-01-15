package databasecodegen

import (
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

type InterleavedRowIteratorCodeGenerator struct {
	Table             *spanddl.Table
	InterleavedTables []*spanddl.Table
}

func (g InterleavedRowIteratorCodeGenerator) Type() string {
	return InterleavedRowCodeGenerator(g).Ident() + "RowIterator"
}

func (g InterleavedRowIteratorCodeGenerator) GenerateCode(f *codegen.File) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	row := InterleavedRowCodeGenerator(g)
	f.P()
	f.P("type ", g.Type(), " struct {")
	f.P("*", spannerPkg, ".RowIterator")
	f.P("}")
	f.P()
	f.P("func (i *", g.Type(), ") Next() (*", row.Type(), ", error) {")
	f.P("spannerRow, err := i.RowIterator.Next()")
	f.P("if err != nil {")
	f.P("return nil, err")
	f.P("}")
	f.P("var row ", row.Type())
	f.P("if err := row.", row.UnmarshalSpannerRowMethod(), "(spannerRow); err != nil {")
	f.P("return nil, err")
	f.P("}")
	f.P("return &row, nil")
	f.P("}")
	f.P()
	f.P("func (i *", g.Type(), ") Do(f func(row *", row.Type(), ") error) error {")
	f.P("return i.RowIterator.Do(func(spannerRow *", spannerPkg, ".Row) error {")
	f.P("var row ", row.Type())
	f.P("if err := row.", row.UnmarshalSpannerRowMethod(), "(spannerRow); err != nil {")
	f.P("return err")
	f.P("}")
	f.P("return f(&row)")
	f.P("})")
	f.P("}")
}
