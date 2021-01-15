package databasecodegen

import (
	"fmt"
	"reflect"
	"strconv"

	"cloud.google.com/go/spanner/spansql"
	"github.com/stoewer/go-strcase"
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/internal/codegen/typescodegen"
	"go.einride.tech/aip-spanner/spanddl"
)

type RowCodeGenerator struct {
	Table *spanddl.Table
}

func (g RowCodeGenerator) Type() string {
	return strcase.UpperCamelCase(string(g.Table.Name)) + "Row"
}

func (g RowCodeGenerator) ColumnFieldName(column *spanddl.Column) string {
	return strcase.UpperCamelCase(string(column.Name))
}

func (g RowCodeGenerator) ColumnNamesMethod() string {
	return "ColumnNames"
}

func (g RowCodeGenerator) ColumnIDsMethod() string {
	return "ColumnIDs"
}

func (g RowCodeGenerator) ColumnExprsMethod() string {
	return "ColumnExprs"
}

func (g RowCodeGenerator) UnmarshalSpannerRowMethod() string {
	return "UnmarshalSpannerRow"
}

func (g RowCodeGenerator) PrimaryKeyMethod() string {
	return "PrimaryKey"
}

func (g RowCodeGenerator) Nil() string {
	return "((*" + g.Type() + ")(nil))"
}

func (g RowCodeGenerator) GenerateCode(f *codegen.File) {
	f.P()
	f.P("type ", g.Type(), " struct {")
	for _, column := range g.Table.Columns {
		g.generateColumn(f, column)
	}
	f.P("}")
	g.generateColumnNamesFunctions(f)
	g.generateValidateFunction(f)
	g.generateUnmarshalFunction(f)
	g.generateMutationFunctions(f)
	g.generateMutationFunction(f)
	g.generateMutationForColumnsFunction(f)
	g.generatePrimaryKeyMethod(f)
}

func (g RowCodeGenerator) generateColumn(f *codegen.File, column *spanddl.Column) {
	f.P(
		g.ColumnFieldName(column), " ", g.columnType(f, column),
		"`spanner:", strconv.Quote(string(column.Name)), "`",
	)
}

func (g RowCodeGenerator) generateValidateFunction(f *codegen.File) {
	fmtPkg := f.Import("fmt")
	f.P()
	f.P("func (r *", g.Type(), ") Validate() error {")
	for _, column := range g.Table.Columns {
		if column.Type.Array && column.NotNull {
			f.P("if r.", g.ColumnFieldName(column), " == nil {")
			f.P(`return `, fmtPkg, `.Errorf("array column `, column.Name, ` is nil")`)
			f.P("}")
		}
		if !column.Type.Array && column.Type.Len > 0 && column.Type.Len != spansql.MaxLen {
			fieldValue := "r." + g.ColumnFieldName(column) + typescodegen.ValueAccessor(column)
			if column.NotNull {
				f.P("if len(", fieldValue, ") > ", column.Type.Len, " {")
				f.P(`return `, fmtPkg, `.Errorf("column `, column.Name, ` length > `, column.Type.Len, `")`)
				f.P("}")
			} else {
				f.P("if !r.", g.ColumnFieldName(column), ".IsNull() && len(", fieldValue, ") > ", column.Type.Len, " {")
				f.P(`return `, fmtPkg, `.Errorf("column `, column.Name, ` length > `, column.Type.Len, `")`)
				f.P("}")
			}
		}
	}
	f.P("return nil")
	f.P("}")
}

func (g RowCodeGenerator) generateUnmarshalFunction(f *codegen.File) {
	fmtPkg := f.Import("fmt")
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (r *", g.Type(), ") UnmarshalSpannerRow(row *", spannerPkg, ".Row) error {")
	f.P("for i := 0; i < row.Size(); i++ {")
	f.P("switch row.ColumnName(i) {")
	for _, column := range g.Table.Columns {
		f.P("case ", strconv.Quote(string(column.Name)), ":")
		f.P("if err := row.Column(i, &r.", g.ColumnFieldName(column), "); err != nil {")
		f.P(`return `, fmtPkg, `.Errorf("unmarshal `, g.Table.Name, ` row: `, column.Name, ` column: %w", err)`)
		f.P("}")
	}
	f.P("default:")
	f.P(`return fmt.Errorf("unmarshal `, g.Table.Name, ` row: unhandled column: %s", row.ColumnName(i))`)
	f.P("}")
	f.P("}")
	f.P("return nil")
	f.P("}")
}

func (g RowCodeGenerator) generateMutationFunctions(f *codegen.File) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (r *", g.Type(), ") Insert() *", spannerPkg, ".Mutation {")
	f.P("return ", spannerPkg, ".Insert(r.Mutation())")
	f.P("}")
	f.P()
	f.P("func (r *", g.Type(), ") InsertOrUpdate() *", spannerPkg, ".Mutation {")
	f.P("return ", spannerPkg, ".InsertOrUpdate(r.Mutation())")
	f.P("}")
	f.P()
	f.P("func (r *", g.Type(), ") Update() *", spannerPkg, ".Mutation {")
	f.P("return ", spannerPkg, ".Update(r.Mutation())")
	f.P("}")
	f.P()
	f.P("func (r *", g.Type(), ") InsertColumns(columns []string) *", spannerPkg, ".Mutation {")
	f.P("return ", spannerPkg, ".Insert(r.MutationForColumns(columns))")
	f.P("}")
	f.P()
	f.P("func (r *", g.Type(), ") InsertOrUpdateColumns(columns []string) *", spannerPkg, ".Mutation {")
	f.P("return ", spannerPkg, ".InsertOrUpdate(r.MutationForColumns(columns))")
	f.P("}")
	f.P()
	f.P("func (r *", g.Type(), ") UpdateColumns(columns []string) *", spannerPkg, ".Mutation {")
	f.P("return ", spannerPkg, ".Update(r.MutationForColumns(columns))")
	f.P("}")
}

func (g RowCodeGenerator) generateMutationFunction(f *codegen.File) {
	f.P()
	f.P("func (r *", g.Type(), ") Mutation() (string, []string, []interface{}) {")
	f.P("return ", strconv.Quote(string(g.Table.Name)), ", r.", g.ColumnNamesMethod(), "(), []interface{}{")
	for _, column := range g.Table.Columns {
		f.P("r.", g.ColumnFieldName(column), ",")
	}
	f.P("}")
	f.P("}")
}

func (g RowCodeGenerator) generateMutationForColumnsFunction(f *codegen.File) {
	fmtPkg := f.Import("fmt")
	f.P()
	f.P("func (r *", g.Type(), ") MutationForColumns(columns []string) (string, []string, []interface{}) {")
	f.P("if len(columns) == 0 {")
	f.P("columns = r.", g.ColumnNamesMethod(), "()")
	f.P("}")
	f.P("values := make([]interface{}, 0, len(columns))")
	f.P("for _, column := range columns {")
	f.P("switch column {")
	for _, column := range g.Table.Columns {
		f.P("case ", strconv.Quote(string(column.Name)), ":")
		f.P("values = append(values, r.", g.ColumnFieldName(column), ")")
	}
	f.P("default:")
	f.P(`panic(`, fmtPkg, `.Errorf("table `, g.Table.Name, ` does not have column %s", column))`)
	f.P("}")
	f.P("}")
	f.P("return ", strconv.Quote(string(g.Table.Name)), ", columns, values")
	f.P("}")
}

func (g RowCodeGenerator) generateColumnNamesFunctions(f *codegen.File) {
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("func (*", g.Type(), ") ", g.ColumnNamesMethod(), "() []string {")
	f.P("return []string{")
	for _, column := range g.Table.Columns {
		f.P(strconv.Quote(string(column.Name)), ",")
	}
	f.P("}")
	f.P("}")
	f.P()
	f.P("func (*", g.Type(), ") ", g.ColumnIDsMethod(), "() []", spansqlPkg, ".ID {")
	f.P("return []", spansqlPkg, ".ID{")
	for _, column := range g.Table.Columns {
		f.P(strconv.Quote(string(column.Name)), ",")
	}
	f.P("}")
	f.P("}")
	f.P()
	f.P("func (*", g.Type(), ") ", g.ColumnExprsMethod(), "() []", spansqlPkg, ".Expr {")
	f.P("return []", spansqlPkg, ".Expr{")
	for _, column := range g.Table.Columns {
		f.P("", spansqlPkg, ".ID(", strconv.Quote(string(column.Name)), "),")
	}
	f.P("}")
	f.P("}")
}

func (g RowCodeGenerator) generatePrimaryKeyMethod(f *codegen.File) {
	primaryKey := KeyCodeGenerator(g)
	f.P()
	f.P("func (r *", g.Type(), ") ", g.PrimaryKeyMethod(), "() ", primaryKey.Type(), " {")
	f.P("return ", primaryKey.Type(), "{")
	for _, keyPart := range g.Table.PrimaryKey {
		f.P(primaryKey.FieldName(keyPart), ": r.", g.ColumnFieldName(g.keyColumn(keyPart)), ",")
	}
	f.P("}")
	f.P("}")
}

func (g RowCodeGenerator) keyColumn(keyPart spansql.KeyPart) *spanddl.Column {
	column, ok := g.Table.Column(keyPart.Column)
	if !ok {
		panic(fmt.Errorf("table %s has no column %s", g.Table.Name, keyPart.Column))
	}
	return column
}

func (g RowCodeGenerator) columnType(f *codegen.File, column *spanddl.Column) reflect.Type {
	t := typescodegen.GoType(column)
	if t.PkgPath() != "" {
		_ = f.Import(t.PkgPath())
	}
	return t
}
