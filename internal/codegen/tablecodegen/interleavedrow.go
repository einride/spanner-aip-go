package tablecodegen

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
	Table             *spanddl.Table
	InterleavedTables []*spanddl.Table
}

func (g InterleavedRowCodeGenerator) Ident() string {
	var t strings.Builder
	_, _ = t.WriteString(strcase.UpperCamelCase(string(g.Table.Name)))
	for _, interleavedTable := range g.InterleavedTables {
		_, _ = t.WriteString("And")
		_, _ = t.WriteString(strcase.UpperCamelCase(string(interleavedTable.Name)))
	}
	return t.String()
}

func (g InterleavedRowCodeGenerator) UnmarshalSpannerRowMethod() string {
	return "UnmarshalSpannerRow"
}

func (g InterleavedRowCodeGenerator) PrimaryKeyMethod() string {
	return "PrimaryKey"
}

func (g InterleavedRowCodeGenerator) Type() string {
	return g.Ident() + "Row"
}

func (g InterleavedRowCodeGenerator) InterleavedRowsField(table *spanddl.Table) string {
	return strcase.UpperCamelCase(string(table.Name))
}

func (g InterleavedRowCodeGenerator) GenerateCode(f *codegen.File) {
	row := RowCodeGenerator{Table: g.Table}
	f.P()
	f.P("type ", g.Type(), " struct {")
	for _, column := range g.Table.Columns {
		row.generateColumn(f, column)
	}
	for _, interleavedTable := range g.InterleavedTables {
		interleavedRow := RowCodeGenerator{Table: interleavedTable}
		f.P(
			g.InterleavedRowsField(interleavedTable), " []*", interleavedRow.Type(),
			"`spanner:", strconv.Quote(string(interleavedTable.Name)), "`",
		)
	}
	f.P("}")
	g.generatePrimaryKeyMethod(f)
	g.generateUnmarshalFunction(f)
}

func (g InterleavedRowCodeGenerator) generateUnmarshalFunction(f *codegen.File) {
	row := RowCodeGenerator{Table: g.Table}
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
	for _, interleavedTable := range g.InterleavedTables {
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

func (g InterleavedRowCodeGenerator) generatePrimaryKeyMethod(f *codegen.File) {
	primaryKey := PrimaryKeyCodeGenerator{g.Table}
	row := RowCodeGenerator{Table: g.Table}
	f.P()
	f.P("func (r *", g.Type(), ") ", g.PrimaryKeyMethod(), "() ", primaryKey.Type(), " {")
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
