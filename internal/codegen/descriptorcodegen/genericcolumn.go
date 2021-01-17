package descriptorcodegen

import (
	"go.einride.tech/spanner-aip/internal/codegen"
)

type GenericColumnDescriptorCodeGenerator struct{}

func (g GenericColumnDescriptorCodeGenerator) InterfaceType() string {
	return "ColumnDescriptor"
}

func (g GenericColumnDescriptorCodeGenerator) StructType() string {
	return "columnDescriptor"
}

func (g GenericColumnDescriptorCodeGenerator) ColumnIDMethod() string {
	return "ColumnID"
}

func (g GenericColumnDescriptorCodeGenerator) ColumnNameMethod() string {
	return "ColumnName"
}

func (g GenericColumnDescriptorCodeGenerator) ColumnExprMethod() string {
	return "ColumnExpr"
}

func (g GenericColumnDescriptorCodeGenerator) ColumnTypeMethod() string {
	return "ColumnType"
}

func (g GenericColumnDescriptorCodeGenerator) NotNullMethod() string {
	return "NotNull"
}

func (g GenericColumnDescriptorCodeGenerator) AllowCommitTimestampMethod() string {
	return "AllowCommitTimestamp"
}

func (g GenericColumnDescriptorCodeGenerator) GenerateCode(f *codegen.File) {
	g.generateInterface(f)
	g.generateStruct(f)
}

func (g GenericColumnDescriptorCodeGenerator) generateInterface(f *codegen.File) {
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.InterfaceType(), " interface {")
	f.P(g.ColumnIDMethod(), "() ", spansqlPkg, ".ID")
	f.P(g.ColumnNameMethod(), "() string")
	f.P(g.ColumnTypeMethod(), "() ", spansqlPkg, ".Type")
	f.P(g.NotNullMethod(), "() bool")
	f.P(g.AllowCommitTimestampMethod(), "() bool")
	f.P("}")
}

func (g GenericColumnDescriptorCodeGenerator) generateStruct(f *codegen.File) {
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.StructType(), " struct {")
	f.P("columnID ", spansqlPkg, ".ID")
	f.P("columnType ", spansqlPkg, ".Type")
	f.P("notNull bool")
	f.P("allowCommitTimestamp bool")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnNameMethod(), "() string {")
	f.P("return string(d.columnID)")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnIDMethod(), "() ", spansqlPkg, ".ID {")
	f.P("return d.columnID")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnTypeMethod(), "() ", spansqlPkg, ".Type {")
	f.P("return d.columnType")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnExprMethod(), "() ", spansqlPkg, ".Expr {")
	f.P("return d.columnID")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.NotNullMethod(), "() bool {")
	f.P("return d.notNull")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.AllowCommitTimestampMethod(), "() bool {")
	f.P("return d.allowCommitTimestamp")
	f.P("}")
}
