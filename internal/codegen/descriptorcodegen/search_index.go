package descriptorcodegen

import (
	"github.com/stoewer/go-strcase"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
)

type SearchIndexDescriptorCodeGenerator struct {
	SeachIndex *spanddl.SearchIndex
}

func (g SearchIndexDescriptorCodeGenerator) InterfaceType() string {
	return strcase.UpperCamelCase(string(g.SeachIndex.Name)) + "SearchIndexDescriptor"
}

func (g SearchIndexDescriptorCodeGenerator) StructType() string {
	return strcase.LowerCamelCase(string(g.SeachIndex.Name)) + "SearchIndexDescriptor"
}

func (g SearchIndexDescriptorCodeGenerator) SearchIndexNameMethod() string {
	return "IndexName"
}

func (g SearchIndexDescriptorCodeGenerator) SearchIndexIDMethod() string {
	return "IndexID"
}

func (g SearchIndexDescriptorCodeGenerator) GenerateCode(f *codegen.File) {
	g.generateInterface(f)
	g.generateStruct(f)
}

func (g SearchIndexDescriptorCodeGenerator) generateInterface(f *codegen.File) {
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.InterfaceType(), " interface {")
	f.P(g.SearchIndexNameMethod(), "() string")
	f.P(g.SearchIndexIDMethod(), "() ", spansqlPkg, ".ID")
	f.P("}")
}

func (g SearchIndexDescriptorCodeGenerator) generateStruct(f *codegen.File) {
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	f.P("type ", g.StructType(), " struct {")
	f.P("indexID ", spansqlPkg, ".ID")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.SearchIndexNameMethod(), "() string {")
	f.P("return string(d.indexID)")
	f.P("}")
	f.P()
	f.P("func (d *", g.StructType(), ") ", g.SearchIndexIDMethod(), "() ", spansqlPkg, ".ID {")
	f.P("return d.indexID")
	f.P("}")
}
