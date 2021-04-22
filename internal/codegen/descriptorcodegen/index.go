package descriptorcodegen

import (
	"strconv"

	"cloud.google.com/go/spanner/spansql"
	"github.com/stoewer/go-strcase"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

type IndexDescriptorCodeGenerator struct {
	Index *spanddl.Index
}

func (g IndexDescriptorCodeGenerator) InterfaceType() string {
	return strcase.UpperCamelCase(string(g.Index.Name)) + "IndexDescriptor"
}

func (g IndexDescriptorCodeGenerator) StructType() string {
	return strcase.LowerCamelCase(string(g.Index.Name)) + "IndexDescriptor"
}

func (g IndexDescriptorCodeGenerator) ColumnDescriptorMethod(keyPart spansql.KeyPart) string {
	return strcase.UpperCamelCase(string(keyPart.Column))
}

func (g IndexDescriptorCodeGenerator) IndexNameMethod() string {
	return "IndexName"
}

func (g IndexDescriptorCodeGenerator) IndexIDMethod() string {
	return "IndexID"
}

func (g IndexDescriptorCodeGenerator) ColumnNamesMethod() string {
	return "ColumnNames"
}

func (g IndexDescriptorCodeGenerator) ColumnIDsMethod() string {
	return "ColumnIDs"
}

func (g IndexDescriptorCodeGenerator) ColumnExprsMethod() string {
	return "ColumnExprs"
}

func (g IndexDescriptorCodeGenerator) GenerateCode(f *codegen.File) {
	g.generateInterface(f)
	g.generateStruct(f)
}

func (g IndexDescriptorCodeGenerator) generateInterface(f *codegen.File) {
	genericColumnDescriptor := GenericColumnDescriptorCodeGenerator{}
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.InterfaceType(), " interface {")
	f.P(g.IndexNameMethod(), "() string")
	f.P(g.IndexIDMethod(), "() ", spansqlPkg, ".ID")
	f.P(g.ColumnNamesMethod(), "() []string")
	f.P(g.ColumnIDsMethod(), "() []", spansqlPkg, ".ID")
	f.P(g.ColumnExprsMethod(), "() []", spansqlPkg, ".Expr")
	for _, column := range g.Index.Columns {
		f.P(g.ColumnDescriptorMethod(column), "() ", genericColumnDescriptor.InterfaceType())
	}
	f.P("}")
}

func (g IndexDescriptorCodeGenerator) generateStruct(f *codegen.File) {
	genericColumnDescriptor := GenericColumnDescriptorCodeGenerator{}
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.StructType(), " struct {")
	f.P("indexID ", spansqlPkg, ".ID")
	for _, column := range g.Index.Columns {
		f.P(g.columnDescriptorField(column), " ", genericColumnDescriptor.StructType())
	}
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.IndexNameMethod(), "() string {")
	f.P("return string(d.indexID)")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.IndexIDMethod(), "() ", spansqlPkg, ".ID {")
	f.P("return d.indexID")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnNamesMethod(), "() []string {")
	f.P("return []string{")
	for _, column := range g.Index.Columns {
		f.P(strconv.Quote(string(column.Column)), ",")
	}
	f.P("}")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnIDsMethod(), "() []", spansqlPkg, ".ID {")
	f.P("return []", spansqlPkg, ".ID{")
	for _, column := range g.Index.Columns {
		f.P(strconv.Quote(string(column.Column)), ",")
	}
	f.P("}")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.ColumnExprsMethod(), "() []", spansqlPkg, ".Expr {")
	f.P("return []", spansqlPkg, ".Expr{")
	for _, column := range g.Index.Columns {
		f.P(spansqlPkg, ".ID(", strconv.Quote(string(column.Column)), "),")
	}
	f.P("}")
	f.P("}")
	for _, column := range g.Index.Columns {
		f.P()
		f.P("func (d *", g.StructType(), ") ", g.ColumnDescriptorMethod(column), "() ColumnDescriptor", " {")
		f.P("return &d.", g.columnDescriptorField(column))
		f.P("}")
	}
}

func (g IndexDescriptorCodeGenerator) columnDescriptorField(keyPart spansql.KeyPart) string {
	return strcase.LowerCamelCase(string(keyPart.Column))
}
