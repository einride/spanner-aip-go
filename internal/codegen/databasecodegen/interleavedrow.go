package databasecodegen

import (
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner/spansql"
	"github.com/stoewer/go-strcase"
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

type InterleavedRowCodeGenerator struct {
	Table *spanddl.Table
}

func (g InterleavedRowCodeGenerator) Ident() string {
	var t strings.Builder
	_, _ = t.WriteString(strcase.UpperCamelCase(string(g.Table.Name)))
	for _, interleavedTable := range g.Table.InterleavedTables {
		_, _ = t.WriteString("And")
		_, _ = t.WriteString(strcase.UpperCamelCase(string(interleavedTable.Name)))
	}
	return t.String()
}

func (g InterleavedRowCodeGenerator) UnmarshalSpannerRowMethod() string {
	return "UnmarshalSpannerRow"
}

func (g InterleavedRowCodeGenerator) KeyMethod() string {
	return "Key"
}

func (g InterleavedRowCodeGenerator) Type() string {
	return g.Ident() + "Row"
}

func (g InterleavedRowCodeGenerator) InterleavedRowsField(table *spanddl.Table) string {
	return strcase.UpperCamelCase(string(table.Name))
}

func (g InterleavedRowCodeGenerator) GenerateCode(f *codegen.File) {
	row := RowCodeGenerator(g)
	f.P()
	f.P("type ", g.Type(), " struct {")
	for _, column := range g.Table.Columns {
		row.generateColumn(f, column)
	}
	for _, interleavedTable := range g.Table.InterleavedTables {
		interleavedRow := RowCodeGenerator{Table: interleavedTable}
		f.P(
			g.InterleavedRowsField(interleavedTable), " []*", interleavedRow.Type(),
			"`spanner:", strconv.Quote(string(interleavedTable.Name)), "`",
		)
	}
	f.P("}")
	g.generateKeyMethod(f)
	g.generateUnmarshalFunction(f)
	g.generateRowMethod(f)
	g.generateInsertMutationMethod(f)
	g.generateUpdateMutationMethod(f)
}

func (g InterleavedRowCodeGenerator) generateInsertMutationMethod(f *codegen.File) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	row := RowCodeGenerator(g)
	f.P()
	f.P("func (r ", g.Type(), ") Insert() []*", spannerPkg, ".Mutation {")
	f.P("n := 1")
	for _, interleavedTable := range g.Table.InterleavedTables {
		f.P("n+=len(r.", g.InterleavedRowsField(interleavedTable), ")")
	}
	f.P("mutations := make([]*", spannerPkg, ".Mutation, 0, n)")
	f.P("mutations = append(mutations, r.", row.Type(), "().Insert())")
	for _, interleavedTable := range g.Table.InterleavedTables {
		f.P("for _, interleavedRow := range r.", g.InterleavedRowsField(interleavedTable), " {")
		f.P("mutations = append(mutations, interleavedRow.Insert())")
		f.P("}")
	}
	f.P("return mutations")
	f.P("}")
}

func (g InterleavedRowCodeGenerator) generateUpdateMutationMethod(f *codegen.File) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	row := RowCodeGenerator(g)
	f.P()
	f.P("func (r ", g.Type(), ") Update() []*", spannerPkg, ".Mutation {")
	f.P("n := ", 1+len(g.Table.InterleavedTables), " // one delete mutation per interleaved table")
	for _, interleavedTable := range g.Table.InterleavedTables {
		f.P("n+=len(r.", g.InterleavedRowsField(interleavedTable), ")")
	}
	f.P("mutations := make([]*", spannerPkg, ".Mutation, 0, n)")
	f.P("mutations = append(mutations, r.", row.Type(), "().Update())")
	for _, interleavedTable := range g.Table.InterleavedTables {
		f.P(
			"mutations = append(mutations, ",
			spannerPkg,
			".Delete(",
			strconv.Quote(string(interleavedTable.Name)),
			", r.Key().SpannerKey().AsPrefix()))",
		)
		f.P("for _, interleavedRow := range r.", g.InterleavedRowsField(interleavedTable), " {")
		f.P("mutations = append(mutations, interleavedRow.Insert())")
		f.P("}")
	}
	f.P("return mutations")
	f.P("}")
}

func (g InterleavedRowCodeGenerator) generateRowMethod(f *codegen.File) {
	row := RowCodeGenerator(g)
	f.P()
	f.P("func (r ", g.Type(), ") ", row.Type(), "() *", row.Type(), " {")
	f.P("return &", row.Type(), "{")
	for _, column := range row.Table.Columns {
		f.P(row.ColumnFieldName(column), ": r.", row.ColumnFieldName(column), ",")
	}
	f.P("}")
	f.P("}")
}

func (g InterleavedRowCodeGenerator) generateUnmarshalFunction(f *codegen.File) {
	row := RowCodeGenerator(g)
	fmtPkg := f.Import("fmt")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (r *", g.Type(), ") ", g.UnmarshalSpannerRowMethod(), "(row *", spannerPkg, ".Row) error {")
	f.P("for i := 0; i < row.Size(); i++ {")
	f.P("switch row.ColumnName(i) {")
	for _, column := range g.Table.Columns {
		f.P("case ", strconv.Quote(string(column.Name)), ":")
		f.P("if err := row.Column(i, &r.", row.ColumnFieldName(column), "); err != nil {")
		f.P(`return `, fmtPkg, `.Errorf("unmarshal `, g.Table.Name, ` row: `, column.Name, ` column: %w", err)`)
		f.P("}")
	}
	for _, interleavedTable := range g.Table.InterleavedTables {
		f.P("case ", strconv.Quote(string(interleavedTable.Name)), ":")
		f.P("if err := row.Column(i, &r.", g.InterleavedRowsField(interleavedTable), "); err != nil {")
		f.P(
			`return `, fmtPkg, `.Errorf("unmarshal `, g.Table.Name, ` interleaved row: `,
			interleavedTable.Name, ` column: %w", err)`,
		)
		f.P("}")
	}
	f.P("default:")
	f.P(`return fmt.Errorf("unmarshal `, g.Table.Name, ` row: unhandled column: %s", row.ColumnName(i))`)
	f.P("}")
	f.P("}")
	f.P("return nil")
	f.P("}")
}

func (g InterleavedRowCodeGenerator) generateKeyMethod(f *codegen.File) {
	primaryKey := KeyCodeGenerator(g)
	row := RowCodeGenerator(g)
	f.P()
	f.P("func (r *", g.Type(), ") ", g.KeyMethod(), "() ", primaryKey.Type(), " {")
	f.P("return ", primaryKey.Type(), "{")
	for _, keyPart := range g.Table.PrimaryKey {
		f.P(primaryKey.FieldName(keyPart), ": r.", row.ColumnFieldName(g.keyColumn(keyPart)), ",")
	}
	f.P("}")
	f.P("}")
}

func (g InterleavedRowCodeGenerator) keyColumn(keyPart spansql.KeyPart) *spanddl.Column {
	column, ok := g.Table.Column(keyPart.Column)
	if !ok {
		panic(fmt.Errorf("table %s has no column %s", g.Table.Name, keyPart.Column))
	}
	return column
}
