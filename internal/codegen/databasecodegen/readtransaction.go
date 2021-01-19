package databasecodegen

import (
	"strconv"

	"github.com/stoewer/go-strcase"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

type ReadTransactionCodeGenerator struct {
	Database *spanddl.Database
}

func (g ReadTransactionCodeGenerator) Type() string {
	return "ReadTransaction"
}

func (g ReadTransactionCodeGenerator) ConstructorMethod() string {
	return "Query"
}

func (g ReadTransactionCodeGenerator) ReadMethod(table *spanddl.Table) string {
	return "Read" + strcase.UpperCamelCase(string(table.Name)) + "Rows"
}

func (g ReadTransactionCodeGenerator) GetMethod(table *spanddl.Table) string {
	return "Get" + strcase.UpperCamelCase(string(table.Name)) + "Row"
}

func (g ReadTransactionCodeGenerator) BatchGetMethod(table *spanddl.Table) string {
	return "BatchGet" + strcase.UpperCamelCase(string(table.Name)) + "Rows"
}

func (g ReadTransactionCodeGenerator) ListMethod(table *spanddl.Table) string {
	return "List" + strcase.UpperCamelCase(string(table.Name)) + "Rows"
}

func (g ReadTransactionCodeGenerator) ListInterleavedMethod(table *spanddl.Table) string {
	return g.ListMethod(table) + "Interleaved"
}

func (g ReadTransactionCodeGenerator) GetInterleavedMethod(table *spanddl.Table) string {
	return g.GetMethod(table) + "Interleaved"
}

func (g ReadTransactionCodeGenerator) BatchGetInterleavedMethod(table *spanddl.Table) string {
	return g.BatchGetMethod(table) + "Interleaved"
}

func (g ReadTransactionCodeGenerator) GenerateCode(f *codegen.File) {
	common := CommonCodeGenerator{}
	f.P()
	f.P("type ", g.Type(), " struct {")
	f.P("Tx ", common.SpannerReadTransactionType())
	f.P("}")
	g.generateConstructorMethod(f)
	for _, table := range g.Database.Tables {
		g.generateReadMethod(f, table)
		g.generateGetMethod(f, table)
		g.generateBatchGetMethod(f, table)
		g.generateListMethod(f, table)
		g.generateListInterleavedMethod(f, table)
		g.generateGetInterleavedMethod(f, table)
		g.generateBatchGetInterleavedMethod(f, table)
	}
}

func (g ReadTransactionCodeGenerator) generateReadMethod(f *codegen.File, table *spanddl.Table) {
	row := RowCodeGenerator{Table: table}
	rowIterator := RowIteratorCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ReadMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("keySet ", spannerPkg, ".KeySet,")
	f.P(") *", rowIterator.Type(), " {")
	f.P("return &", rowIterator.Type(), "{")
	f.P("RowIterator: t.Tx.Read(")
	f.P("ctx,")
	f.P(strconv.Quote(string(table.Name)), ",")
	f.P("keySet,")
	f.P(row.Nil(), ".", row.ColumnNamesMethod(), "(),")
	f.P("),")
	f.P("}")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateGetMethod(f *codegen.File, table *spanddl.Table) {
	key := KeyCodeGenerator{Table: table}
	row := RowCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.GetMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("key ", key.Type(), ",")
	f.P(") (*", row.Type(), ", error) {")
	f.P("spannerRow, err := t.Tx.ReadRow(")
	f.P("ctx,")
	f.P(strconv.Quote(string(table.Name)), ",")
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

func (g ReadTransactionCodeGenerator) generateBatchGetMethod(f *codegen.File, table *spanddl.Table) {
	contextPkg := f.Import("context")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	key := KeyCodeGenerator{Table: table}
	row := RowCodeGenerator{Table: table}
	f.P()
	f.P("func (t ", g.Type(), ") ", g.BatchGetMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("keys []", key.Type(), ",")
	f.P(") (map[", key.Type(), "]*", row.Type(), ", error) {")
	f.P("spannerKeys := make([]", spannerPkg, ".KeySet, 0, len(keys))")
	f.P("for _, key := range keys {")
	f.P("spannerKeys = append(spannerKeys, key.SpannerKey())")
	f.P("}")
	f.P("foundRows := make(map[", key.Type(), "]*", row.Type(), ", len(keys))")
	f.P(
		"if err := t.", g.ReadMethod(table), "(ctx, ", spannerPkg, ".KeySets(spannerKeys...))",
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

func (g ReadTransactionCodeGenerator) generateListMethod(f *codegen.File, table *spanddl.Table) {
	const (
		limitParam  = "limit"
		offsetParam = "offset"
	)
	rowIterator := RowIteratorCodeGenerator{Table: table}
	row := RowCodeGenerator{Table: table}
	key := KeyCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ListMethod(table), "(")
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
	f.P("", spansqlPkg, ".SelectFromTable{Table: ", strconv.Quote(string(table.Name)), "},")
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

func (g ReadTransactionCodeGenerator) generateListInterleavedMethod(f *codegen.File, table *spanddl.Table) {
	const (
		limitParam  = "limit"
		offsetParam = "offset"
	)
	rowIterator := RowIteratorCodeGenerator{Table: table}
	key := KeyCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	stringsPkg := f.Import("strings")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ListInterleavedMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ListQuery,")
	f.P(") *", rowIterator.Type(), " {")
	f.P("if len(query.Order) == 0 {")
	f.P("query.Order = ", key.Type(), "{}.Order()")
	f.P("}")
	f.P("var q ", stringsPkg, ".Builder")
	f.P(`_, _ = q.WriteString("SELECT ")`)
	for _, column := range table.Columns {
		f.P(`_, _ = q.WriteString("`, column.Name, `, ")`)
	}
	for _, interleavedTable := range table.InterleavedTables {
		f.P(`_, _ = q.WriteString("ARRAY( ")`)
		f.P(`_, _ = q.WriteString("SELECT AS STRUCT ")`)
		for _, column := range interleavedTable.Columns {
			f.P(`_, _ = q.WriteString("`, column.Name, `, ")`)
		}
		f.P(`_, _ = q.WriteString("FROM `, interleavedTable.Name, ` ")`)
		f.P(`_, _ = q.WriteString("WHERE ")`)
		for i, keyPart := range table.PrimaryKey {
			f.P(`_, _ = q.WriteString("`, keyPart.Column, ` = `, table.Name, `.`, keyPart.Column, ` ")`)
			if i < len(table.PrimaryKey)-1 {
				f.P(`_, _ = q.WriteString("AND ")`)
			}
		}
		f.P(`_, _ = q.WriteString("ORDER BY ")`)
		for i, keyPart := range interleavedTable.PrimaryKey {
			f.P(`_, _ = q.WriteString("`, keyPart.Column, `")`)
			if keyPart.Desc {
				f.P(`_, _ = q.WriteString(" DESC")`)
			}
			if i < len(interleavedTable.PrimaryKey)-1 {
				f.P(`_, _ = q.WriteString(", ")`)
			} else {
				f.P(`_, _ = q.WriteString(" ")`)
			}
		}
		f.P(`_, _ = q.WriteString(") AS `, interleavedTable.Name, `, ")`)
	}
	f.P(`_, _ = q.WriteString("FROM `, table.Name, ` ")`)
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

func (g ReadTransactionCodeGenerator) generateGetInterleavedMethod(f *codegen.File, table *spanddl.Table) {
	key := KeyCodeGenerator{Table: table}
	row := RowCodeGenerator{Table: table}
	common := CommonCodeGenerator{}
	contextPkg := f.Import("context")
	iteratorPkg := f.Import("google.golang.org/api/iterator")
	codesPkg := f.Import("google.golang.org/grpc/codes")
	statusPkg := f.Import("google.golang.org/grpc/status")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.GetInterleavedMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("key ", key.Type(), ",")
	f.P(") (*", row.Type(), ", error) {")
	f.P("it := t.", g.ListInterleavedMethod(table), "(ctx, ", common.ListQueryType(), "{")
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

func (g ReadTransactionCodeGenerator) generateBatchGetInterleavedMethod(f *codegen.File, table *spanddl.Table) {
	key := KeyCodeGenerator{Table: table}
	row := RowCodeGenerator{Table: table}
	common := CommonCodeGenerator{}
	contextPkg := f.Import("context")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.BatchGetInterleavedMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("keys []", key.Type(), ",")
	f.P(") (map[", key.Type(), "]*", row.Type(), ", error) {")
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
	f.P("foundRows := make(map[", key.Type(), "]*", row.Type(), ", len(keys))")
	f.P("if err := t.", g.ListInterleavedMethod(table), "(ctx, ", common.ListQueryType(), "{")
	f.P("Where: ", spansqlPkg, ".Paren{Expr: where},")
	f.P("Limit: int32(len(keys)),")
	f.P("}).Do(func(row *", row.Type(), ") error {")
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
