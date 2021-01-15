package databasecodegen

import (
	"strconv"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

type InterleavedReadTransactionCodeGenerator struct {
	Table             *spanddl.Table
	InterleavedTables []*spanddl.Table
}

func (g InterleavedReadTransactionCodeGenerator) Type() string {
	return InterleavedRowCodeGenerator(g).Ident() + "ReadTransaction"
}

func (g InterleavedReadTransactionCodeGenerator) ConstructorMethod() string {
	return InterleavedRowCodeGenerator(g).Ident()
}

func (g InterleavedReadTransactionCodeGenerator) ListMethod() string {
	return "List"
}

func (g InterleavedReadTransactionCodeGenerator) GetMethod() string {
	return "Get"
}

func (g InterleavedReadTransactionCodeGenerator) BatchGetMethod() string {
	return "BatchGet"
}

func (g InterleavedReadTransactionCodeGenerator) GenerateCode(f *codegen.File) {
	common := CommonCodeGenerator{}
	f.P()
	f.P("type ", g.Type(), " struct {")
	f.P("Tx ", common.SpannerReadTransactionType())
	f.P("}")
	g.generateConstructorMethod(f)
	g.generateListRowsMethod(f)
	g.generateGetRowMethod(f)
	g.generateBatchGetRowsMethod(f)
}

func (g InterleavedReadTransactionCodeGenerator) generateListRowsMethod(f *codegen.File) {
	const (
		limitParam  = "limit"
		offsetParam = "offset"
	)
	rowIterator := InterleavedRowIteratorCodeGenerator(g)
	contextPkg := f.Import("context")
	stringsPkg := f.Import("strings")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ListMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ListQuery,")
	f.P(") *", rowIterator.Type(), " {")
	f.P("var q ", stringsPkg, ".Builder")
	f.P(`_, _ = q.WriteString("SELECT ")`)
	for _, column := range g.Table.Columns {
		f.P(`_, _ = q.WriteString("`, column.Name, `, ")`)
	}
	for _, interleavedTable := range g.InterleavedTables {
		f.P(`_, _ = q.WriteString("ARRAY( ")`)
		f.P(`_, _ = q.WriteString("SELECT AS STRUCT ")`)
		for _, column := range interleavedTable.Columns {
			f.P(`_, _ = q.WriteString("`, column.Name, `, ")`)
		}
		f.P(`_, _ = q.WriteString("FROM `, interleavedTable.Name, ` ")`)
		f.P(`_, _ = q.WriteString("WHERE ")`)
		for i, keyPart := range g.Table.PrimaryKey {
			f.P(`_, _ = q.WriteString("`, keyPart.Column, ` = `, g.Table.Name, `.`, keyPart.Column, ` ")`)
			if i < len(g.Table.PrimaryKey)-1 {
				f.P(`_, _ = q.WriteString("AND ")`)
			}
		}
		f.P(`_, _ = q.WriteString(") AS `, interleavedTable.Name, `, ")`)
	}
	f.P(`_, _ = q.WriteString("FROM `, g.Table.Name, ` ")`)
	f.P("if query.Where != nil {")
	f.P(`_, _ = q.WriteString("WHERE (")`)
	f.P(`_, _ = q.WriteString(query.Where.SQL())`)
	f.P(`_, _ = q.WriteString(") ")`)
	f.P("}")
	f.P("if len(query.Order) > 0 {")
	f.P(`_, _ = q.WriteString("ORDER BY ")`)
	f.P(`for i, order := range query.Order {`)
	f.P(`_, _ = q.WriteString(order.SQL())`)
	f.P("if i < len(query.Order) - 1 {")
	f.P(`_, _ = q.WriteString(", ")`)
	f.P("}")
	f.P(`}`)
	f.P("}")
	f.P(`_, _ = q.WriteString("LIMIT @`, limitParam, ` ")`)
	f.P(`_, _ = q.WriteString("OFFSET @`, offsetParam, ` ")`)
	f.P("stmt := ", spannerPkg, ".Statement{")
	f.P("SQL: q.String(),")
	f.P("Params: map[string]interface{}{")
	f.P(strconv.Quote(limitParam), ": query.Limit,")
	f.P(strconv.Quote(offsetParam), ": query.Offset,")
	f.P("},")
	f.P("}")
	f.P("return &", rowIterator.Type(), "{")
	f.P("RowIterator: t.Tx.Query(ctx, stmt),")
	f.P("}")
	f.P("}")
}

func (g InterleavedReadTransactionCodeGenerator) generateGetRowMethod(f *codegen.File) {
	primaryKey := KeyCodeGenerator{Table: g.Table}
	row := InterleavedRowCodeGenerator(g)
	common := CommonCodeGenerator{}
	contextPkg := f.Import("context")
	iteratorPkg := f.Import("google.golang.org/api/iterator")
	codesPkg := f.Import("google.golang.org/grpc/codes")
	statusPkg := f.Import("google.golang.org/grpc/status")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.GetMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("key ", primaryKey.Type(), ",")
	f.P(") (*", row.Type(), ", error) {")
	f.P("it := t.List(ctx, ", common.ListQueryType(), "{")
	f.P("Where: key.BoolExpr(),")
	f.P("Limit: 1,")
	f.P("})")
	f.P("defer it.Stop()")
	f.P("row, err := it.Next()")
	f.P("if err != nil {")
	f.P("if err == ", iteratorPkg, ".Done {")
	f.P(`return nil, `, statusPkg, `.Errorf(`, codesPkg, `.NotFound, "not found: %v", key)`)
	f.P("}")
	f.P("return nil, err")
	f.P("}")
	f.P("return row, nil")
	f.P("}")
}

func (g InterleavedReadTransactionCodeGenerator) generateBatchGetRowsMethod(f *codegen.File) {
	primaryKey := KeyCodeGenerator{Table: g.Table}
	interleavedRow := InterleavedRowCodeGenerator(g)
	common := CommonCodeGenerator{}
	contextPkg := f.Import("context")
	codesPkg := f.Import("google.golang.org/grpc/codes")
	statusPkg := f.Import("google.golang.org/grpc/status")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.BatchGetMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("keys []", primaryKey.Type(), ",")
	f.P(") ([]*", interleavedRow.Type(), ", error) {")
	f.P("if len(keys) == 0 {")
	f.P("return nil, nil")
	f.P("}")
	f.P("where := keys[0].BoolExpr()")
	f.P("for _, key := range keys[1:] {")
	f.P("where = ", spansqlPkg, ".LogicalOp{")
	f.P("Op: ", spansqlPkg, ".Or,")
	f.P("LHS: where,")
	f.P("RHS: key.BoolExpr(),")
	f.P("}")
	f.P("}")
	f.P("it := t.List(ctx, ", common.ListQueryType(), "{")
	f.P("Where: ", spansqlPkg, ".Paren{Expr: where},")
	f.P("Limit: int64(len(keys)),")
	f.P("})")
	f.P("defer it.Stop()")
	f.P("foundRows := make(map[", primaryKey.Type(), "]*", interleavedRow.Type(), ", len(keys))")
	f.P("if err := it.Do(func(row *", interleavedRow.Type(), ") error {")
	f.P("foundRows[row.", interleavedRow.PrimaryKeyMethod(), "()] = row")
	f.P("return nil")
	f.P("}); err != nil {")
	f.P("return nil, err")
	f.P("}")
	f.P("rows := make([]*", interleavedRow.Type(), ", 0, len(keys))")
	f.P("for _, key := range keys {")
	f.P("row, ok := foundRows[key]")
	f.P("if !ok {")
	f.P(`return nil, `, statusPkg, `.Errorf(`, codesPkg, `.NotFound, "not found: %v", key)`)
	f.P("}")
	f.P("rows = append(rows, row)")
	f.P("}")
	f.P("return rows, nil")
	f.P("}")
}

func (g InterleavedReadTransactionCodeGenerator) generateConstructorMethod(f *codegen.File) {
	common := CommonCodeGenerator{}
	f.P()
	f.P("func ", g.ConstructorMethod(), "(tx ", common.SpannerReadTransactionType(), ") ", g.Type(), " {")
	f.P("return ", g.Type(), "{Tx: tx}")
	f.P("}")
}
