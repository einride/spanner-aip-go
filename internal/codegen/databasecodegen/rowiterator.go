package databasecodegen

import (
	"github.com/stoewer/go-strcase"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

type RowIteratorCodeGenerator struct {
	Table *spanddl.Table
}

func (g RowIteratorCodeGenerator) InterfaceType() string {
	return strcase.UpperCamelCase(string(g.Table.Name)) + "RowIterator"
}

func (g RowIteratorCodeGenerator) StreamingType() string {
	return "streaming" + strcase.UpperCamelCase(string(g.Table.Name)) + "RowIterator"
}

func (g RowIteratorCodeGenerator) BufferedType() string {
	return "buffered" + strcase.UpperCamelCase(string(g.Table.Name)) + "RowIterator"
}

func (g RowIteratorCodeGenerator) GenerateCode(f *codegen.File) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	iteratorPkg := f.Import("google.golang.org/api/iterator")
	row := RowCodeGenerator(g)
	f.P()
	f.P("type ", g.InterfaceType(), " interface {")
	f.P("Next() (*", row.Type(), ", error)")
	f.P("Do(f func(row *", row.Type(), ") error) error")
	f.P("Stop()")
	f.P("Count() int64")
	f.P("}")
	f.P()
	f.P("type ", g.StreamingType(), " struct {")
	f.P("*", spannerPkg, ".RowIterator")
	f.P("}")
	f.P()
	f.P("func (i *", g.StreamingType(), ") Next() (*", row.Type(), ", error) {")
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
	f.P("func (i *", g.StreamingType(), ") Do(f func(row *", row.Type(), ") error) error {")
	f.P("return i.RowIterator.Do(func(spannerRow *", spannerPkg, ".Row) error {")
	f.P("var row ", row.Type())
	f.P("if err := row.", row.UnmarshalSpannerRowMethod(), "(spannerRow); err != nil {")
	f.P("return err")
	f.P("}")
	f.P("return f(&row)")
	f.P("})")
	f.P("}")
	f.P()
	f.P("func (i *", g.StreamingType(), ") Count() int64 {")
	f.P("return i.RowCount")
	f.P("}")
	f.P()
	f.P("type ", g.BufferedType(), " struct {")
	f.P("rows []*", row.Type())
	f.P("err error")
	f.P("}")
	f.P()
	f.P("func (i *", g.BufferedType(), ") Next() (*", row.Type(), ", error) {")
	f.P("if i.err != nil {")
	f.P("return nil, i.err")
	f.P("}")
	f.P("if len(i.rows) == 0 {")
	f.P("return nil, ", iteratorPkg, ".Done")
	f.P("}")
	f.P("next := i.rows[0]")
	f.P("i.rows = i.rows[1:]")
	f.P("return next, nil")
	f.P("}")
	f.P()
	f.P("func (i *", g.BufferedType(), ") Count() int64 {")
	f.P("return int64(len(i.rows))")
	f.P("}")
	f.P()
	f.P("func (i *", g.BufferedType(), ") Do(f func(row *", row.Type(), ") error) error {")
	f.P("for {")
	f.P("row, err := i.Next()")
	f.P("switch err {")
	f.P("case ", iteratorPkg, ".Done:")
	f.P("return nil")
	f.P("case nil:")
	f.P("if err = f(row); err != nil {")
	f.P("return err")
	f.P("}")
	f.P("default:")
	f.P("return err")
	f.P("}")
	f.P("}")
	f.P("}")
	f.P()
	f.P("func (i *", g.BufferedType(), ") Stop() {}")
}
