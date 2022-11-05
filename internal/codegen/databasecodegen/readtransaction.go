package databasecodegen

import (
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

func (g ReadTransactionCodeGenerator) ReadInterleavedMethod(table *spanddl.Table) string {
	return "readInterleaved" + strcase.UpperCamelCase(string(table.Name)) + "Rows"
}

func (g ReadTransactionCodeGenerator) ReadInterleavedQuery(table *spanddl.Table) string {
	return g.ReadInterleavedMethod(table) + "Query"
}

func (g ReadTransactionCodeGenerator) ReadInterleavedResult(table *spanddl.Table) string {
	return g.ReadInterleavedMethod(table) + "Result"
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
			g.generateReadInterleavedRowsQuery(f, table)
			g.generateReadInterleavedRowsResult(f, table)
			g.generateReadInterleavedRowsMethod(f, table)
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
	if len(table.InterleavedTables) == 0 {
		f.P("return &row, nil")
		f.P("}")
		return
	}
	f.P("if !query.hasInterleavedTables() {")
	f.P("return &row, nil")
	f.P("}")
	f.P("interleaved, err := t.", g.ReadInterleavedMethod(table), "(ctx, ", g.ReadInterleavedQuery(table), "{")
	f.P("KeySet: row.Key().SpannerKey().AsPrefix(),")
	g.forwardInterleavedTablesStructFields(f, table, "query")
	f.P("})")
	f.P("if err != nil {")
	f.P("return nil, err")
	f.P("}")
	for _, child := range table.InterleavedTables {
		childName := strcase.UpperCamelCase(string(child.Name))
		f.P("if rs, ok := interleaved.", childName, "[row.Key()]; ok {")
		f.P("row.", childName, " = rs")
		f.P("}")
	}
	f.P("return &row, nil")
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
	f.P("spannerKeys := make([]", spannerPkg, ".KeySet, 0, len(query.Keys))")
	f.P("spannerPrefixKeys :=  make([]", spannerPkg, ".KeySet, 0, len(query.Keys))")
	f.P("for _, key := range query.Keys {")
	f.P("spannerKeys = append(spannerKeys, key.SpannerKey())")
	f.P("spannerPrefixKeys = append(spannerPrefixKeys, key.SpannerKey().AsPrefix())")
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
	if len(table.InterleavedTables) == 0 {
		f.P("return foundRows, nil")
		f.P("}")
		return
	}
	f.P("if !query.hasInterleavedTables() {")
	f.P("return foundRows, nil")
	f.P("}")
	f.P("interleaved, err := t.", g.ReadInterleavedMethod(table), "(ctx, ", g.ReadInterleavedQuery(table), "{")
	f.P("KeySet: ", spannerPkg, ".KeySets(spannerPrefixKeys...),")
	g.forwardInterleavedTablesStructFields(f, table, "query")
	f.P("})")
	f.P("if err != nil {")
	f.P("return nil, err")
	f.P("}")
	f.P("for _, row := range foundRows {")
	for _, child := range table.InterleavedTables {
		childName := strcase.UpperCamelCase(string(child.Name))
		f.P("if rs, ok := interleaved.", childName, "[row.Key()]; ok {")
		f.P("row.", childName, " = rs")
		f.P("}")
	}
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
	f.P("iter := &", rowIterator.StreamingType(), "{")
	f.P("RowIterator: t.Tx.Query(ctx, stmt),")
	f.P("}")
	if len(table.InterleavedTables) == 0 {
		f.P("return iter")
		f.P("}")
		return
	}
	f.P("if !query.hasInterleavedTables() {")
	f.P("return iter")
	f.P("}")
	f.P("rows := make([]*", row.Type(), ", 0, query.Limit)")
	f.P("lookup := make(map[", key.Type(), "]*", row.Type(), ", query.Limit)")
	f.P("prefixes :=  make([]", spannerPkg, ".KeySet, 0, query.Limit)")
	f.P("if err := iter.Do(func(row *", row.Type(), ") error {")
	f.P("k := row.Key()")
	f.P("rows = append(rows, row)")
	f.P("lookup[k] = row")
	f.P("prefixes = append(prefixes, k.SpannerKey().AsPrefix())")
	f.P("return nil")
	f.P("}); err != nil {")
	f.P("return &", rowIterator.BufferedType(), "{ err: err }")
	f.P("}")
	f.P("interleaved, err := t.", g.ReadInterleavedMethod(table), "(ctx, ", g.ReadInterleavedQuery(table), "{")
	f.P("KeySet: ", spannerPkg, ".KeySets(prefixes...),")
	g.forwardInterleavedTablesStructFields(f, table, "query")
	f.P("})")
	f.P("if err != nil {")
	f.P("return &", rowIterator.BufferedType(), "{ err: err }")
	f.P("}")
	f.P("for key, row := range lookup {")
	for _, child := range table.InterleavedTables {
		childName := strcase.UpperCamelCase(string(child.Name))
		f.P("if rs, ok := interleaved.", childName, "[key]; ok {")
		f.P("row.", childName, " = rs")
		f.P("}")
	}
	f.P("}")
	f.P("return &", rowIterator.BufferedType(), "{ rows: rows }")
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateReadInterleavedRowsQuery(f *codegen.File, table *spanddl.Table) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("type ", g.ReadInterleavedQuery(table), " struct {")
	f.P("KeySet ", spannerPkg, ".KeySet")
	g.generateInterleavedTablesStructFields(f, table)
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateReadInterleavedRowsResult(f *codegen.File, table *spanddl.Table) {
	key := KeyCodeGenerator{Table: table}
	f.P()
	f.P("type ", g.ReadInterleavedResult(table), " struct {")
	for _, child := range table.InterleavedTables {
		f.P(strcase.UpperCamelCase(string(child.Name)), " map[", key.Type(), "][]*", RowCodeGenerator{Table: child}.Type())
	}
	f.P("}")
}

func (g ReadTransactionCodeGenerator) generateReadInterleavedRowsMethod(f *codegen.File, table *spanddl.Table) {
	ctxPkg := f.Import("context")
	reflectPkg := f.Import("reflect")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	key := KeyCodeGenerator{Table: table}
	f.P("func (t ", g.Type(), ") ", g.ReadInterleavedMethod(table), "(")
	f.P("ctx ", ctxPkg, ".Context,")
	f.P("query ", g.ReadInterleavedQuery(table), ",")
	f.P(") (*", g.ReadInterleavedResult(table), ", error) {")
	f.P("var r ", g.ReadInterleavedResult(table))
	rangeInterleavedTables(table, func(parent, child *spanddl.Table) {
		isTopLevel := parent.Name == table.Name
		key := KeyCodeGenerator{Table: child}
		name := strcase.UpperCamelCase(string(child.Name))
		row := RowCodeGenerator{Table: child}
		if len(child.InterleavedTables) > 0 {
			// lookup for tables interleaved in this one.
			f.P("interleaved", name, "Lookup:=make(map[", key.Type(), "]*", row.Type(), ")")
		}
		if !isTopLevel {
			// read order of rows in this table.
			// top level tables are inserted directly in response.
			f.P("interleaved", name, ":=make([]*", row.Type(), ", 0)")
		}
	})
	rangeInterleavedTables(table, func(parent, child *spanddl.Table) {
		isTopLevel := parent.Name == table.Name
		parentKey := KeyCodeGenerator{Table: parent}
		row := RowCodeGenerator{Table: child}
		childName := strcase.UpperCamelCase(string(child.Name))
		// If the parent query does not return any data, we need to avoid querying the interleaved table because
		// spanner does not support querying with no keys.
		// Since the KeySet interface contains no public methods to get the list of keys, we use the reflect package
		// to compare the given key with a set of empty keys.
		f.P("if query.", childName, " && !", reflectPkg, ".DeepEqual(query.KeySet, ", spannerPkg, ".KeySets()) {")
		if isTopLevel {
			f.P("r.", childName, " = make(map[", key.Type(), "][]*", row.Type(), ")")
		}
		f.P("if err := t.", g.ReadMethod(child), "(ctx, query.KeySet).Do(func(row *", row.Type(), ") error {")
		if isTopLevel {
			f.P("k := ", parentKey.Type(), "{")
			for _, part := range parent.PrimaryKey {
				f.P(key.FieldName(part), ": row.", key.FieldName(part), ",")
			}
			f.P("}")
			f.P("r.", childName, "[k] = append(r.", childName, "[k], row)")
		} else {
			f.P("interleaved", childName, " = append(interleaved", childName, ", row)")
		}
		if len(child.InterleavedTables) > 0 {
			f.P("interleaved", childName, "Lookup[row.Key()] = row")
		}
		f.P("return nil")
		f.P("}); err != nil {")
		f.P("return nil, err")
		f.P("}")
		f.P("}")
	})
	rangeInterleavedTables(table, func(parent, child *spanddl.Table) {
		isTopLevel := parent.Name == table.Name
		if isTopLevel {
			return
		}
		parentName := strcase.UpperCamelCase(string(parent.Name))
		childName := strcase.UpperCamelCase(string(child.Name))
		parentKey := KeyCodeGenerator{Table: parent}
		f.P("for _, row := range interleaved", childName, "{")
		f.P("k := ", parentKey.Type(), "{")
		for _, part := range parent.PrimaryKey {
			f.P(key.FieldName(part), ": row.", key.FieldName(part), ",")
		}
		f.P("}")
		f.P("if p, ok := interleaved", parentName, "Lookup[k]; ok {")
		f.P("p.", childName, " = append(p.", childName, ", row)")
		f.P("}")
		f.P("}")
	})
	f.P("return &r, nil")
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

func rangeInterleavedTables(table *spanddl.Table, f func(parent, child *spanddl.Table)) {
	for _, interleaved := range table.InterleavedTables {
		f(table, interleaved)
		rangeInterleavedTables(interleaved, f)
	}
}
