package databasecodegen

import (
	"go.einride.tech/spanner-aip/internal/codegen"
)

type CommonCodeGenerator struct{}

func (g CommonCodeGenerator) SpannerReadTransactionType() string {
	return "SpannerReadTransaction"
}

func (g CommonCodeGenerator) GenerateCode(f *codegen.File) {
	g.generateSpannerReadTransactionInterface(f)
}

func (g CommonCodeGenerator) generateSpannerReadTransactionInterface(f *codegen.File) {
	contextPkg := f.Import("context")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("type ", g.SpannerReadTransactionType(), " interface {")
	f.P(
		"Read(ctx ", contextPkg, ".Context, table string, keys ", spannerPkg, ".KeySet, columns []string) *",
		spannerPkg, ".RowIterator",
	)
	f.P(
		"ReadUsingIndex(ctx ", contextPkg, ".Context, table, index string, keys ", spannerPkg, ".KeySet, columns []string) *",
		spannerPkg, ".RowIterator",
	)
	f.P(
		"ReadRow(ctx ", contextPkg, ".Context, table string, key ", spannerPkg, ".Key, columns []string) (*",
		spannerPkg, ".Row, error)",
	)
	f.P(
		"ReadRowUsingIndex(ctx ", contextPkg, ".Context, table string, index string, key ", spannerPkg,
		".Key, columns []string) (*", spannerPkg, ".Row, error)",
	)
	f.P("Query(ctx ", contextPkg, ".Context, statement ", spannerPkg, ".Statement) *", spannerPkg, ".RowIterator")
	f.P("}")
}
