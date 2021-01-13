package descriptorcodegen

import (
	"fmt"
	"strconv"

	"github.com/stoewer/go-strcase"
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

type DatabaseDescriptorCodeGenerator struct {
	Database *spanddl.Database
}

func (g DatabaseDescriptorCodeGenerator) DescriptorFunction() string {
	return "Descriptor"
}

func (g DatabaseDescriptorCodeGenerator) InterfaceType() string {
	return "DatabaseDescriptor"
}

func (g DatabaseDescriptorCodeGenerator) StructType() string {
	return "databaseDescriptor"
}

func (g DatabaseDescriptorCodeGenerator) TableDescriptorMethod(table *spanddl.Table) string {
	return strcase.UpperCamelCase(string(table.Name))
}

func (g DatabaseDescriptorCodeGenerator) GenerateCode(f *codegen.File) {
	g.generateGlobalFunction(f)
	g.generateGlobalVariable(f)
	g.generateInterface(f)
	g.generateStruct(f)
	for _, table := range g.Database.Tables {
		TableDescriptorCodeGenerator{Table: table}.GenerateCode(f)
	}
	GenericColumnDescriptorCodeGenerator{}.GenerateCode(f)
}

func (g DatabaseDescriptorCodeGenerator) generateGlobalFunction(f *codegen.File) {
	f.P()
	f.P("func ", g.DescriptorFunction(), "() ", g.InterfaceType(), " {")
	f.P("return &", g.globalVariableName())
	f.P("}")
}

func (g DatabaseDescriptorCodeGenerator) generateInterface(f *codegen.File) {
	f.P()
	f.P("type ", g.InterfaceType(), " interface {")
	for _, table := range g.Database.Tables {
		tableDescriptor := TableDescriptorCodeGenerator{Table: table}
		f.P(g.TableDescriptorMethod(table), "() ", tableDescriptor.InterfaceType())
	}
	f.P("}")
}

func (g DatabaseDescriptorCodeGenerator) generateStruct(f *codegen.File) {
	f.P()
	f.P("type ", g.StructType(), " struct {")
	for _, table := range g.Database.Tables {
		tableDescriptor := TableDescriptorCodeGenerator{Table: table}
		f.P(g.tableDescriptorField(table), " ", tableDescriptor.StructType())
	}
	f.P("}")
	for _, table := range g.Database.Tables {
		tableDescriptor := TableDescriptorCodeGenerator{Table: table}
		f.P()
		f.P(
			"func (d *", g.StructType(), ") ",
			g.TableDescriptorMethod(table), "() ", tableDescriptor.InterfaceType(), " {",
		)
		f.P("return &d.", g.tableDescriptorField(table))
		f.P("}")
	}
}

func (g DatabaseDescriptorCodeGenerator) generateGlobalVariable(f *codegen.File) {
	f.P()
	f.P("var ", g.globalVariableName(), " = ", g.StructType(), "{")
	for _, table := range g.Database.Tables {
		tableDescriptor := TableDescriptorCodeGenerator{Table: table}
		f.P(g.tableDescriptorField(table), ": ", tableDescriptor.StructType(), "{")
		f.P("tableID: ", strconv.Quote(string(table.Name)), ",")
		for _, column := range table.Columns {
			columnDescriptor := GenericColumnDescriptorCodeGenerator{}
			f.P(g.columnDescriptorField(column), ": ", columnDescriptor.StructType(), "{")
			f.P("columnID: ", strconv.Quote(string(column.Name)), ",")
			f.P("columnType: ", fmt.Sprintf("%#v", column.Type), ",")
			f.P("notNull: ", column.NotNull, ",")
			allowCommitTimestamp := column.Options.AllowCommitTimestamp != nil && *column.Options.AllowCommitTimestamp
			f.P("allowCommitTimestamp: ", allowCommitTimestamp, ",")
			f.P("},")
		}
		f.P("},")
	}
	f.P("}")
}

func (g DatabaseDescriptorCodeGenerator) tableDescriptorField(table *spanddl.Table) string {
	return strcase.LowerCamelCase(string(table.Name))
}

func (g DatabaseDescriptorCodeGenerator) columnDescriptorField(column *spanddl.Column) string {
	return strcase.LowerCamelCase(string(column.Name))
}

func (g DatabaseDescriptorCodeGenerator) globalVariableName() string {
	return "descriptor"
}
