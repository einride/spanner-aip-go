package databasecodegen

import (
	"strconv"
	"strings"

	"github.com/stoewer/go-strcase"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

type ParentReadTransactionCodeGenerator struct {
	Table *spanddl.Table
}

func (g ParentReadTransactionCodeGenerator) Type() string {
	return strcase.UpperCamelCase(string(g.Table.Name)) + "ParentReadTransaction"
}

func (g ParentReadTransactionCodeGenerator) ConstructorMethod() string {
	return strcase.UpperCamelCase(string(g.Table.Name)) + "Parent"
}

func (g ParentReadTransactionCodeGenerator) ListMethod() string {
	return "List"
}

func (g ParentReadTransactionCodeGenerator) GetMethod() string {
	return "Get"
}

func (g ParentReadTransactionCodeGenerator) BatchGetMethod() string {
	return "BatchGet"
}

func (g ParentReadTransactionCodeGenerator) GenerateCode(f *codegen.File) {
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

func (g ParentReadTransactionCodeGenerator) generateListRowsMethod(f *codegen.File) {
	const (
		limitParam  = "limit"
		offsetParam = "offset"
	)
	rowIterator := ParentRowIteratorCodeGenerator(g)
	key := KeyCodeGenerator(g)
	contextPkg := f.Import("context")
	stringsPkg := f.Import("strings")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ListMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ListQuery,")
	f.P(") *", rowIterator.Type(), " {")
	f.P("if len(query.Order) == 0 {")
	f.P("query.Order = ", key.Type(), "{}.Order()")
	f.P("}")
	f.P("var q ", stringsPkg, ".Builder")
	f.P("_, _ = q.WriteString(`")
	t := func(level int) string {
		return strings.Repeat(" ", level*4)
	}
	f.P(t(0), "SELECT")
	for _, column := range g.Table.Columns {
		f.P(t(1), column.Name, ",")
	}
	var interleave func(level int, parent, child *spanddl.Table)
	interleave = func(l int, parent, child *spanddl.Table) {
		f.P(t(l), "ARRAY(")
		f.P(t(l+1), "SELECT AS STRUCT")
		for _, column := range child.Columns {
			f.P(t(l+2), column.Name, ",")
		}
		for _, grandChild := range child.InterleavedTables {
			interleave(l+2, child, grandChild)
		}
		f.P(t(l+1), "FROM ")
		f.P(t(l+2), child.Name)
		f.P(t(l+1), "WHERE ")
		for i, keyPart := range parent.PrimaryKey {
			var and string
			if i < len(parent.PrimaryKey)-1 {
				and = " AND"
			}
			f.P(t(l+2), child.Name, ".", keyPart.Column, " = ", parent.Name, ".", keyPart.Column, and)
		}
		f.P(t(l+1), "ORDER BY ")
		for i, keyPart := range child.PrimaryKey {
			var comma string
			if i < len(child.PrimaryKey)-1 {
				comma = ","
			}
			var desc string
			if keyPart.Desc {
				desc = " DESC"
			}
			f.P(t(l+2), keyPart.Column, desc, comma)
		}
		f.P(t(l), ") AS ", child.Name, ",")
	}
	for _, child := range g.Table.InterleavedTables {
		interleave(1, g.Table, child)
	}
	f.P(t(0), "FROM")
	f.P(t(1), g.Table.Name)
	f.P("`)")
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
	f.P("} else {")
	f.P(`_, _ = q.WriteString(" ")`)
	f.P("}")
	f.P(`}`)
	f.P("}")
	f.P(`_, _ = q.WriteString("LIMIT @`, limitParam, ` ")`)
	f.P(`_, _ = q.WriteString("OFFSET @`, offsetParam, ` ")`)
	f.P("stmt := ", spannerPkg, ".Statement{")
	f.P("SQL: q.String(),")
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

func (g ParentReadTransactionCodeGenerator) generateGetRowMethod(f *codegen.File) {
	primaryKey := KeyCodeGenerator(g)
	row := ParentRowCodeGenerator(g)
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

func (g ParentReadTransactionCodeGenerator) generateBatchGetRowsMethod(f *codegen.File) {
	primaryKey := KeyCodeGenerator(g)
	interleavedRow := ParentRowCodeGenerator(g)
	common := CommonCodeGenerator{}
	contextPkg := f.Import("context")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.BatchGetMethod(), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("keys []", primaryKey.Type(), ",")
	f.P(") (map[", primaryKey.Type(), "]*", interleavedRow.Type(), ", error) {")
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
	f.P("foundRows := make(map[", primaryKey.Type(), "]*", interleavedRow.Type(), ", len(keys))")
	f.P("if err := t.List(ctx, ", common.ListQueryType(), "{")
	f.P("Where: ", spansqlPkg, ".Paren{Expr: where},")
	f.P("Limit: int32(len(keys)),")
	f.P("}).Do(func(row *", interleavedRow.Type(), ") error {")
	f.P("foundRows[row.", interleavedRow.KeyMethod(), "()] = row")
	f.P("return nil")
	f.P("}); err != nil {")
	f.P("return nil, err")
	f.P("}")
	f.P("return foundRows, nil")
	f.P("}")
}

func (g ParentReadTransactionCodeGenerator) generateConstructorMethod(f *codegen.File) {
	common := CommonCodeGenerator{}
	f.P()
	f.P("func ", g.ConstructorMethod(), "(tx ", common.SpannerReadTransactionType(), ") ", g.Type(), " {")
	f.P("return ", g.Type(), "{Tx: tx}")
	f.P("}")
}
