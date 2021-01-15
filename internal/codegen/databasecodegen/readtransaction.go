package databasecodegen

import (
	"strconv"

	"github.com/stoewer/go-strcase"
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

type ReadTransactionCodeGenerator struct {
	Table *spanddl.Table
}

func (g ReadTransactionCodeGenerator) Type() string {
	return strcase.UpperCamelCase(string(g.Table.Name)) + "ReadTransaction"
}

func (g ReadTransactionCodeGenerator) ConstructorMethod() string {
	return strcase.UpperCamelCase(string(g.Table.Name))
}

func (g ReadTransactionCodeGenerator) ReadMethod() string {
	return "Read"
}

func (g ReadTransactionCodeGenerator) ListMethod() string {
	return "List"
}

func (g ReadTransactionCodeGenerator) GetMethod() string {
	return "Get"
}

func (g ReadTransactionCodeGenerator) BatchGetMethod() string {
	return "BatchGet"
}

func (g ReadTransactionCodeGenerator) GenerateCode(f *codegen.File) {
	common := CommonCodeGenerator{}
	f.P()
	f.P("type ", g.Type(), " struct {")
	f.P("Tx ", common.SpannerReadTransactionType())
	f.P("}")
	g.generateConstructorMethod(f)
	g.generateReadRowsMethod(f)
	g.generateGetRowMethod(f)
	g.generateBatchGetRowsMethod(f)
	g.generateListRowsMethod(f)
}

func (g ReadTransactionCodeGenerator) generateReadRowsMethod(f *codegen.File) {
	rowIterator := RowIteratorCodeGenerator(g)
	row := RowCodeGenerator(g)
	contextPkg := f.Import("context")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ReadMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("keySet ", spannerPkg, ".KeySet,")
	f.P(") *", rowIterator.Type(), " {")
	f.P("return &", rowIterator.Type(), "{")
	f.P("RowIterator: t.Tx.Read(")
	f.P("ctx,")
	f.P(strconv.Quote(string(g.Table.Name)), ",")
	f.P("keySet,")
	f.P(row.Nil(), ".", row.ColumnNamesMethod(), "(),")
	f.P("),")
	f.P("}")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateListRowsMethod(f *codegen.File) {
	const (
		limitParam  = "limit"
		offsetParam = "offset"
	)
	rowIterator := RowIteratorCodeGenerator(g)
	row := RowCodeGenerator(g)
	key := KeyCodeGenerator(g)
	contextPkg := f.Import("context")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ListMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ListQuery,")
	f.P(") *", rowIterator.Type(), " {")
	f.P("if len(query.Order) == 0 {")
	f.P("query.Order = ", key.Type(), "{}.Order()")
	f.P("}")
	f.P("stmt := ", spannerPkg, ".Statement{")
	f.P("SQL: ", spansqlPkg, ".Query{")
	f.P("Select: ", spansqlPkg, ".Select{")
	f.P("List: ", row.Nil(), ".", row.ColumnExprsMethod(), "(),")
	f.P("From: []", spansqlPkg, ".SelectFrom{")
	f.P("", spansqlPkg, ".SelectFromTable{Table: ", strconv.Quote(string(g.Table.Name)), "},")
	f.P("},")
	f.P("Where: query.Where,")
	f.P("},")
	f.P("Order:  query.Order,")
	f.P("Limit:  ", spansqlPkg, ".Param(", strconv.Quote(limitParam), "),")
	f.P("Offset: ", spansqlPkg, ".Param(", strconv.Quote(offsetParam), "),")
	f.P("}.SQL(),")
	f.P("Params: map[string]interface{}{")
	f.P(strconv.Quote(limitParam), ": int64(query.Limit),")
	f.P(strconv.Quote(offsetParam), ": query.Offset,")
	f.P("},")
	f.P("}")
	f.P("return &", rowIterator.Type(), "{")
	f.P("RowIterator: t.Tx.Query(ctx, stmt),")
	f.P("}")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateGetRowMethod(f *codegen.File) {
	primaryKey := KeyCodeGenerator(g)
	row := RowCodeGenerator(g)
	contextPkg := f.Import("context")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.GetMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("key ", primaryKey.Type(), ",")
	f.P(") (*", row.Type(), ", error) {")
	f.P("spannerRow, err := t.Tx.ReadRow(")
	f.P("ctx,")
	f.P(strconv.Quote(string(g.Table.Name)), ",")
	f.P("key.SpannerKey(),")
	f.P(row.Nil(), ".", row.ColumnNamesMethod(), "(),")
	f.P(")")
	f.P("if err != nil {")
	f.P("return nil, err")
	f.P("}")
	f.P("var row ", row.Type())
	f.P("if err := row.", row.UnmarshalSpannerRowMethod(), "(spannerRow); err != nil {")
	f.P("return nil, err")
	f.P("}")
	f.P("return &row, nil")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateBatchGetRowsMethod(f *codegen.File) {
	contextPkg := f.Import("context")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	primaryKey := KeyCodeGenerator(g)
	row := RowCodeGenerator(g)
	f.P()
	f.P("func (t ", g.Type(), ") ", g.BatchGetMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("keys []", primaryKey.Type(), ",")
	f.P(") (map[", primaryKey.Type(), "]*", row.Type(), ", error) {")
	f.P("spannerKeys := make([]", spannerPkg, ".KeySet, 0, len(keys))")
	f.P("for _, key := range keys {")
	f.P("spannerKeys = append(spannerKeys, key.SpannerKey())")
	f.P("}")
	f.P("foundRows := make(map[", primaryKey.Type(), "]*", row.Type(), ", len(keys))")
	f.P(
		"if err := t.", g.ReadMethod(), "(ctx, ", spannerPkg, ".KeySets(spannerKeys...))",
		".Do(func(row *", row.Type(), ") error {",
	)
	f.P("foundRows[row.", row.KeyMethod(), "()] = row")
	f.P("return nil")
	f.P("}); err != nil {")
	f.P("return nil, err")
	f.P("}")
	f.P("return foundRows, nil")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateConstructorMethod(f *codegen.File) {
	common := CommonCodeGenerator{}
	f.P()
	f.P("func ", g.ConstructorMethod(), "(tx ", common.SpannerReadTransactionType(), ") ", g.Type(), " {")
	f.P("return ", g.Type(), "{Tx: tx}")
	f.P("}")
}
