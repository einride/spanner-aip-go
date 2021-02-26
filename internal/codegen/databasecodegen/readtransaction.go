package databasecodegen

import (
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner/spansql"
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

func (g ReadTransactionCodeGenerator) ListQueryStruct(table *spanddl.Table) string {
	return g.ListMethod(table) + "Query"
}

func (g ReadTransactionCodeGenerator) GetQueryStruct(table *spanddl.Table) string {
	return g.GetMethod(table) + "Query"
}

func (g ReadTransactionCodeGenerator) BatchGetQueryStruct(table *spanddl.Table) string {
	return g.BatchGetMethod(table) + "Query"
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
	return strcase.LowerCamelCase(g.ListMethod(table)) + "Interleaved"
}

func (g ReadTransactionCodeGenerator) GetInterleavedMethod(table *spanddl.Table) string {
	return strcase.LowerCamelCase(g.GetMethod(table)) + "Interleaved"
}

func (g ReadTransactionCodeGenerator) BatchGetInterleavedMethod(table *spanddl.Table) string {
	return strcase.LowerCamelCase(g.BatchGetMethod(table)) + "Interleaved"
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
		g.generateGetQueryStruct(f, table)
		g.generateGetMethod(f, table)
		g.generateBatchGetQueryStruct(f, table)
		g.generateBatchGetMethod(f, table)
		g.generateListQueryStruct(f, table)
		g.generateListMethod(f, table)
		if len(table.InterleavedTables) > 0 {
			g.generateListInterleavedMethod(f, table)
			g.generateGetInterleavedMethod(f, table)
			g.generateBatchGetInterleavedMethod(f, table)
		}
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
	f.P(") ", rowIterator.InterfaceType(), " {")
	f.P("return &", rowIterator.StreamingType(), "{")
	f.P("RowIterator: t.Tx.Read(")
	f.P("ctx,")
	f.P(strconv.Quote(string(table.Name)), ",")
	f.P("keySet,")
	f.P(row.Nil(), ".", row.ColumnNamesMethod(), "(),")
	f.P("),")
	f.P("}")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateGetQueryStruct(f *codegen.File, table *spanddl.Table) {
	key := KeyCodeGenerator{Table: table}
	f.P()
	f.P("type ", g.GetQueryStruct(table), " struct {")
	f.P("Key ", key.Type())
	g.generateInterleavedTablesStructFields(f, table)
	f.P("}")
	if len(table.InterleavedTables) > 0 {
		f.P()
		f.P("func (q *", g.GetQueryStruct(table), ") hasInterleavedTables() bool {")
		f.P("return ", hasInterleavedTablesPredicate("q", table))
		f.P("}")
	}
}

func (g ReadTransactionCodeGenerator) generateGetMethod(f *codegen.File, table *spanddl.Table) {
	row := RowCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.GetMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ", g.GetQueryStruct(table), ",")
	f.P(") (*", row.Type(), ", error) {")
	if len(table.InterleavedTables) > 0 {
		f.P("if query.hasInterleavedTables() {")
		f.P("return t.", g.GetInterleavedMethod(table), "(ctx, query)")
		f.P("}")
	}
	f.P("spannerRow, err := t.Tx.ReadRow(")
	f.P("ctx,")
	f.P(strconv.Quote(string(table.Name)), ",")
	f.P("query.Key.SpannerKey(),")
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

func (g ReadTransactionCodeGenerator) generateGetInterleavedMethod(f *codegen.File, table *spanddl.Table) {
	row := RowCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	iteratorPkg := f.Import("google.golang.org/api/iterator")
	codesPkg := f.Import("google.golang.org/grpc/codes")
	statusPkg := f.Import("google.golang.org/grpc/status")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.GetInterleavedMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ", g.GetQueryStruct(table), ",")
	f.P(") (*", row.Type(), ", error) {")
	f.P("it := t.", g.ListInterleavedMethod(table), "(ctx, ", g.ListQueryStruct(table), "{")
	f.P("Limit: 1,")
	f.P("Where: query.Key.BoolExpr(),")
	if g.hasSoftDelete(table) {
		f.P("ShowDeleted: true,")
	}
	g.forwardInterleavedTablesStructFields(f, table, "query")
	f.P("})")
	f.P("defer it.Stop()")
	f.P("row, err := it.Next()")
	f.P("if err != nil {")
	f.P("if err == ", iteratorPkg, ".Done {")
	f.P(`return nil, `, statusPkg, `.Errorf(`, codesPkg, `.NotFound, "not found: %v", query.Key)`)
	f.P("}")
	f.P("return nil, err")
	f.P("}")
	f.P("return row, nil")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateBatchGetQueryStruct(f *codegen.File, table *spanddl.Table) {
	key := KeyCodeGenerator{Table: table}
	f.P()
	f.P("type ", g.BatchGetQueryStruct(table), " struct {")
	f.P("Keys  []", key.Type())
	g.generateInterleavedTablesStructFields(f, table)
	f.P("}")
	if len(table.InterleavedTables) > 0 {
		f.P()
		f.P("func (q *", g.BatchGetQueryStruct(table), ") hasInterleavedTables() bool {")
		f.P("return ", hasInterleavedTablesPredicate("q", table))
		f.P("}")
	}
}

func (g ReadTransactionCodeGenerator) generateBatchGetMethod(f *codegen.File, table *spanddl.Table) {
	contextPkg := f.Import("context")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	key := KeyCodeGenerator{Table: table}
	row := RowCodeGenerator{Table: table}
	f.P()
	f.P("func (t ", g.Type(), ") ", g.BatchGetMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ", g.BatchGetQueryStruct(table), ",")
	f.P(") (map[", key.Type(), "]*", row.Type(), ", error) {")
	if len(table.InterleavedTables) > 0 {
		f.P("if query.hasInterleavedTables() {")
		f.P("return t.", g.BatchGetInterleavedMethod(table), "(ctx, query)")
		f.P("}")
	}
	f.P("spannerKeys := make([]", spannerPkg, ".KeySet, 0, len(query.Keys))")
	f.P("for _, key := range query.Keys {")
	f.P("spannerKeys = append(spannerKeys, key.SpannerKey())")
	f.P("}")
	f.P("foundRows := make(map[", key.Type(), "]*", row.Type(), ", len(query.Keys))")
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

func (g ReadTransactionCodeGenerator) generateListQueryStruct(f *codegen.File, table *spanddl.Table) {
	f.P()
	f.P("type ", g.ListQueryStruct(table), " struct {")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P("Where  ", spansqlPkg, ".BoolExpr")
	f.P("Order  []", spansqlPkg, ".Order")
	f.P("Limit  int32")
	f.P("Offset int64")
	f.P("Params map[string]interface{}")
	if g.hasSoftDelete(table) {
		f.P("ShowDeleted bool")
	}
	g.generateInterleavedTablesStructFields(f, table)
	f.P("}")
	if len(table.InterleavedTables) > 0 {
		f.P()
		f.P("func (q *", g.ListQueryStruct(table), ") hasInterleavedTables() bool {")
		f.P("return ", hasInterleavedTablesPredicate("q", table))
		f.P("}")
	}
}

func (g ReadTransactionCodeGenerator) generateListMethod(f *codegen.File, table *spanddl.Table) {
	const (
		limitParam  = "__limit"
		offsetParam = "__offset"
	)
	rowIterator := RowIteratorCodeGenerator{Table: table}
	row := RowCodeGenerator{Table: table}
	key := KeyCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	fmtPkg := f.Import("fmt")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ListMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ", g.ListQueryStruct(table), ",")
	f.P(") ", rowIterator.InterfaceType(), " {")
	if len(table.InterleavedTables) > 0 {
		f.P("if query.hasInterleavedTables() {")
		f.P("return t.", g.ListInterleavedMethod(table), "(ctx, query)")
		f.P("}")
	}
	f.P("if len(query.Order) == 0 {")
	f.P("query.Order = ", key.Type(), "{}.Order()")
	f.P("}")
	f.P("params := make(map[string]interface{}, len(query.Params)+2)")
	f.P("params[", strconv.Quote(limitParam), "] = int64(query.Limit)")
	f.P("params[", strconv.Quote(offsetParam), "] = int64(query.Offset)")
	f.P("for param, value := range query.Params {")
	f.P("if _, ok := params[param]; ok {")
	f.P("panic(", fmtPkg, `.Errorf("invalid param: %s", param))`)
	f.P("}")
	f.P("params[param] = value")
	f.P("}")
	f.P("if query.Where == nil {")
	f.P("query.Where = ", spansqlPkg, ".True")
	f.P("}")
	if g.hasSoftDelete(table) {
		f.P("if !query.ShowDeleted {")
		f.P("query.Where = ", spansqlPkg, ".LogicalOp{")
		f.P("Op: ", spansqlPkg, ".And,")
		f.P("LHS: ", spansqlPkg, ".Paren{Expr: query.Where},")
		f.P("RHS: ", spansqlPkg, ".IsOp{")
		f.P("LHS: ", spansqlPkg, ".ID(", strconv.Quote(string(g.softDeleteTimestampColumnName(table))), "),")
		f.P("RHS: ", spansqlPkg, ".Null,")
		f.P("},")
		f.P("}")
		f.P("}")
	}
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
	f.P("Params: params,")
	f.P("}")
	f.P("return &", rowIterator.StreamingType(), "{")
	f.P("RowIterator: t.Tx.Query(ctx, stmt),")
	f.P("}")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateListInterleavedMethod(f *codegen.File, table *spanddl.Table) {
	const (
		limitParam  = "__limit"
		offsetParam = "__offset"
	)
	rowIterator := RowIteratorCodeGenerator{Table: table}
	key := KeyCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	fmtPkg := f.Import("fmt")
	stringsPkg := f.Import("strings")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.ListInterleavedMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ", g.ListQueryStruct(table), ",")
	f.P(") ", rowIterator.InterfaceType(), " {")
	f.P("if len(query.Order) == 0 {")
	f.P("query.Order = ", key.Type(), "{}.Order()")
	f.P("}")
	f.P("var q ", stringsPkg, ".Builder")
	f.P("_, _ = q.WriteString(`")
	t := func(level int) string {
		return strings.Repeat(" ", level*4)
	}
	f.P(t(0), "SELECT")
	for _, column := range table.Columns {
		f.P(t(1), column.Name, ",")
	}
	f.P("`)")
	var interleave func(level int, parent, child *spanddl.Table)
	interleave = func(l int, parent, child *spanddl.Table) {
		f.P("if query.", strcase.UpperCamelCase(string(child.Name)), " {")
		f.P("_, _ = q.WriteString(`")
		f.P(t(l), "ARRAY(")
		f.P(t(l+1), "SELECT AS STRUCT")
		for _, column := range child.Columns {
			f.P(t(l+2), column.Name, ",")
		}
		f.P("`)")
		for _, grandChild := range child.InterleavedTables {
			interleave(l+2, child, grandChild)
		}
		f.P("_, _ = q.WriteString(`")
		f.P(t(l+1), "FROM ")
		f.P(t(l+2), child.Name)
		f.P(t(l+1), "WHERE ")
		if g.hasSoftDelete(child) {
			f.P("`)")
			f.P("if !query.ShowDeleted {")
			f.P("_, _ = q.WriteString(`")
			f.P(t(l+2), g.softDeleteTimestampColumnName(child), " IS NULL AND")
			f.P("`)")
			f.P("}")
			f.P("_, _ = q.WriteString(`")
		}
		for i, keyPart := range parent.PrimaryKey {
			var and string
			if i < len(parent.PrimaryKey)-1 {
				and = " AND"
			}
			if keyColumn(child, keyPart).NotNull {
				f.P(t(l+2), child.Name, ".", keyPart.Column, " = ", parent.Name, ".", keyPart.Column, and)
			} else {
				// comparing null with null (null = null) returns a "falsy" value in spanner
				f.P(
					t(l+2), "((", child.Name, ".", keyPart.Column, " IS NULL AND ", parent.Name, ".", keyPart.Column,
					" IS NULL) OR ", child.Name, ".", keyPart.Column, " = ", parent.Name, ".", keyPart.Column, ")", and,
				)
			}
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
		f.P("`)")
		f.P("}")
	}
	for _, child := range table.InterleavedTables {
		interleave(1, table, child)
	}
	f.P("_, _ = q.WriteString(`")
	f.P(t(0), "FROM")
	f.P(t(1), table.Name)
	f.P("`)")
	f.P("if query.Where == nil {")
	f.P("query.Where = ", spansqlPkg, ".True")
	f.P("}")
	if g.hasSoftDelete(table) {
		f.P("if !query.ShowDeleted {")
		f.P("query.Where = ", spansqlPkg, ".LogicalOp{")
		f.P("Op: ", spansqlPkg, ".And,")
		f.P("LHS: ", spansqlPkg, ".Paren{Expr: query.Where},")
		f.P("RHS: ", spansqlPkg, ".IsOp{")
		f.P("LHS: ", spansqlPkg, ".ID(", strconv.Quote(string(g.softDeleteTimestampColumnName(table))), "),")
		f.P("RHS: ", spansqlPkg, ".Null,")
		f.P("},")
		f.P("}")
		f.P("}")
	}
	f.P(`_, _ = q.WriteString("WHERE (")`)
	f.P(`_, _ = q.WriteString(query.Where.SQL())`)
	f.P(`_, _ = q.WriteString(") ")`)
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
	f.P("params := make(map[string]interface{}, len(query.Params)+2)")
	f.P("params[", strconv.Quote(limitParam), "] = int64(query.Limit)")
	f.P("params[", strconv.Quote(offsetParam), "] = int64(query.Offset)")
	f.P("for param, value := range query.Params {")
	f.P("if _, ok := params[param]; ok {")
	f.P("panic(", fmtPkg, `.Errorf("invalid param: %s", param))`)
	f.P("}")
	f.P("params[param] = value")
	f.P("}")
	f.P("stmt := ", spannerPkg, ".Statement{")
	f.P("SQL: q.String(),")
	f.P("Params: params,")
	f.P("}")
	f.P("return &", rowIterator.StreamingType(), "{")
	f.P("RowIterator: t.Tx.Query(ctx, stmt),")
	f.P("}")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateBatchGetInterleavedMethod(f *codegen.File, table *spanddl.Table) {
	key := KeyCodeGenerator{Table: table}
	row := RowCodeGenerator{Table: table}
	contextPkg := f.Import("context")
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (t ", g.Type(), ") ", g.BatchGetInterleavedMethod(table), "(")
	f.P("ctx ", contextPkg, ".Context,")
	f.P("query ", g.BatchGetQueryStruct(table), ",")
	f.P(") (map[", key.Type(), "]*", row.Type(), ", error) {")
	f.P("if len(query.Keys) == 0 {")
	f.P("return nil, nil")
	f.P("}")
	f.P("where := query.Keys[0].BoolExpr()")
	f.P("for _, key := range query.Keys[1:] {")
	f.P("where = ", spansqlPkg, ".LogicalOp{")
	f.P("Op: ", spansqlPkg, ".Or,")
	f.P("LHS: where,")
	f.P("RHS: key.BoolExpr(),")
	f.P("}")
	f.P("}")
	f.P("foundRows := make(map[", key.Type(), "]*", row.Type(), ", len(query.Keys))")
	f.P("if err := t.", g.ListMethod(table), "(ctx, ", g.ListQueryStruct(table), "{")
	f.P("Where: ", spansqlPkg, ".Paren{Expr: where},")
	f.P("Limit: int32(len(query.Keys)),")
	if g.hasSoftDelete(table) {
		f.P("ShowDeleted: true,")
	}
	g.forwardInterleavedTablesStructFields(f, table, "query")
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

func hasInterleavedTablesPredicate(field string, table *spanddl.Table) string {
	var variables []string
	var addTable func(table *spanddl.Table)
	addTable = func(table *spanddl.Table) {
		variables = append(variables, field+"."+strcase.UpperCamelCase(string(table.Name)))
		for _, interleavedTable := range table.InterleavedTables {
			addTable(interleavedTable)
		}
	}
	for _, interleavedTable := range table.InterleavedTables {
		addTable(interleavedTable)
	}
	return strings.Join(variables, " || ")
}

func (g ReadTransactionCodeGenerator) generateInterleavedTablesStructFields(f *codegen.File, table *spanddl.Table) {
	var pTable func(table *spanddl.Table)
	pTable = func(table *spanddl.Table) {
		f.P(strcase.UpperCamelCase(string(table.Name)), " bool")
		for _, interleavedTable := range table.InterleavedTables {
			pTable(interleavedTable)
		}
	}
	for _, interleavedTable := range table.InterleavedTables {
		pTable(interleavedTable)
	}
}

func (g ReadTransactionCodeGenerator) forwardInterleavedTablesStructFields(
	f *codegen.File,
	table *spanddl.Table,
	field string,
) {
	var pTable func(table *spanddl.Table)
	pTable = func(table *spanddl.Table) {
		f.P(strcase.UpperCamelCase(string(table.Name)), ": ", field, ".", strcase.UpperCamelCase(string(table.Name)), ",")
		for _, interleavedTable := range table.InterleavedTables {
			pTable(interleavedTable)
		}
	}
	for _, interleavedTable := range table.InterleavedTables {
		pTable(interleavedTable)
	}
}

func (g ReadTransactionCodeGenerator) hasSoftDelete(table *spanddl.Table) bool {
	for _, column := range table.Columns {
		if column.Name == g.softDeleteTimestampColumnName(table) &&
			!column.NotNull &&
			column.Type == (spansql.Type{Base: spansql.Timestamp}) {
			return true
		}
	}
	return false
}

func (g ReadTransactionCodeGenerator) softDeleteTimestampColumnName(table *spanddl.Table) spansql.ID {
	return "delete_time"
}

func keyColumn(table *spanddl.Table, keyPart spansql.KeyPart) *spanddl.Column {
	column, ok := table.Column(keyPart.Column)
	if !ok {
		panic(fmt.Errorf("table %s has no column %s", table.Name, keyPart.Column))
	}
	return column
}
