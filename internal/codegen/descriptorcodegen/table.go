package descriptorcodegen

import (
	"strconv"

	"github.com/stoewer/go-strcase"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

type TableDescriptorCodeGenerator struct {
	Table *spanddl.Table
}

func (g TableDescriptorCodeGenerator) InterfaceType() string {
	return strcase.UpperCamelCase(string(g.Table.Name)) + "TableDescriptor"
}

func (g TableDescriptorCodeGenerator) StructType() string {
	return strcase.LowerCamelCase(string(g.Table.Name)) + "TableDescriptor"
}

func (g TableDescriptorCodeGenerator) ColumnDescriptorMethod(column *spanddl.Column) string {
	return strcase.UpperCamelCase(string(column.Name))
}

func (g TableDescriptorCodeGenerator) TableNameMethod() string {
	return "TableName"
}

func (g TableDescriptorCodeGenerator) TableIDMethod() string {
	return "TableID"
}

func (g TableDescriptorCodeGenerator) ColumnNamesMethod() string {
	return "ColumnNames"
}

func (g TableDescriptorCodeGenerator) ColumnIDsMethod() string {
	return "ColumnIDs"
}

func (g TableDescriptorCodeGenerator) ColumnExprsMethod() string {
	return "ColumnExprs"
}

func (g TableDescriptorCodeGenerator) GenerateCode(f *codegen.File) {
	g.generateInterface(f)
	g.generateStruct(f)
}

func (g TableDescriptorCodeGenerator) generateInterface(f *codegen.File) {
	genericColumnDescriptor := GenericColumnDescriptorCodeGenerator{}
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.InterfaceType(), " interface {")
	f.P(g.TableNameMethod(), "() string")
	f.P(g.TableIDMethod(), "() ", spansqlPkg, ".ID")
	f.P(g.ColumnNamesMethod(), "() []string")
	f.P(g.ColumnIDsMethod(), "() []", spansqlPkg, ".ID")
	for _, column := range g.Table.Columns {
		f.P(g.ColumnDescriptorMethod(column), "() ", genericColumnDescriptor.InterfaceType())
	}
	f.P("}")
}

func (g TableDescriptorCodeGenerator) generateStruct(f *codegen.File) {
	genericColumnDescriptor := GenericColumnDescriptorCodeGenerator{}
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.StructType(), " struct {")
	f.P("tableID ", spansqlPkg, ".ID")
	for _, column := range g.Table.Columns {
		f.P(g.columnDescriptorField(column), " ", genericColumnDescriptor.StructType())
	}
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.TableNameMethod(), "() string {")
	f.P("return string(d.tableID)")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.TableIDMethod(), "() ", spansqlPkg, ".ID {")
	f.P("return d.tableID")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnNamesMethod(), "() []string {")
	f.P("return []string{")
	for _, column := range g.Table.Columns {
		f.P(strconv.Quote(string(column.Name)), ",")
	}
	f.P("}")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnIDsMethod(), "() []", spansqlPkg, ".ID {")
	f.P("return []", spansqlPkg, ".ID{")
	for _, column := range g.Table.Columns {
		f.P(strconv.Quote(string(column.Name)), ",")
	}
	f.P("}")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnExprsMethod(), "() []", spansqlPkg, ".Expr {")
	f.P("return []", spansqlPkg, ".Expr{")
	for _, column := range g.Table.Columns {
		f.P(spansqlPkg, ".ID(", strconv.Quote(string(column.Name)), "),")
	}
	f.P("}")
	f.P("}")
	for _, column := range g.Table.Columns {
		f.P()
		f.P("func (d *", g.StructType(), ") ", g.ColumnDescriptorMethod(column), "() ColumnDescriptor", " {")
		f.P("return &d.", g.columnDescriptorField(column))
		f.P("}")
	}
}

func (g TableDescriptorCodeGenerator) columnDescriptorField(column *spanddl.Column) string {
	return strcase.LowerCamelCase(string(column.Name))
}
