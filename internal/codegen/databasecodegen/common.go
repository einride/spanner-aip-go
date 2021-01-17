package databasecodegen

import (
	"go.einride.tech/spanner-aip/internal/codegen"
)

type CommonCodeGenerator struct{}

func (g CommonCodeGenerator) ListQueryType() string {
	return "ListQuery"
}

func (g CommonCodeGenerator) SpannerReadTransactionType() string {
	return "SpannerReadTransaction"
}

func (g CommonCodeGenerator) GenerateCode(f *codegen.File) {
	g.generateListQueryStruct(f)
	g.generateSpannerReadTransactionInterface(f)
}

func (g CommonCodeGenerator) generateListQueryStruct(f *codegen.File) {
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.ListQueryType(), " struct {")
	f.P("Where  ", spansqlPkg, ".BoolExpr")
	f.P("Order  []", spansqlPkg, ".Order")
	f.P("Limit  int32")
	f.P("Offset int64")
	f.P("}")
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
		"ReadRow(ctx ", contextPkg, ".Context, table string, key ", spannerPkg, ".Key, columns []string) (*",
		spannerPkg, ".Row, error)",
	)
	f.P("Query(ctx ", contextPkg, ".Context, statement ", spannerPkg, ".Statement) *", spannerPkg, ".RowIterator")
	f.P("}")
}
