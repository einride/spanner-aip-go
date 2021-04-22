package descriptorcodegen

import (
	"fmt"
	"strconv"

	"cloud.google.com/go/spanner/spansql"
	"github.com/stoewer/go-strcase"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
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

func (g DatabaseDescriptorCodeGenerator) IndexDescriptorMethod(index *spanddl.Index) string {
	return strcase.UpperCamelCase(string(index.Name))
}

func (g DatabaseDescriptorCodeGenerator) GenerateCode(f *codegen.File) {
	g.generateGlobalFunction(f)
	g.generateGlobalVariable(f)
	g.generateInterface(f)
	g.generateStruct(f)
	for _, table := range g.Database.Tables {
		TableDescriptorCodeGenerator{Table: table}.GenerateCode(f)
	}
	for _, index := range g.Database.Indexes {
		IndexDescriptorCodeGenerator{Index: index}.GenerateCode(f)
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
	for _, index := range g.Database.Indexes {
		indexDescriptor := IndexDescriptorCodeGenerator{Index: index}
		f.P(g.IndexDescriptorMethod(index), "() ", indexDescriptor.InterfaceType())
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
	for _, index := range g.Database.Indexes {
		indexDescriptor := IndexDescriptorCodeGenerator{Index: index}
		f.P(g.indexDescriptorField(index), " ", indexDescriptor.StructType())
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
	for _, index := range g.Database.Indexes {
		indexDescriptor := IndexDescriptorCodeGenerator{Index: index}
		f.P()
		f.P(
			"func (d *", g.StructType(), ") ",
			g.IndexDescriptorMethod(index), "() ", indexDescriptor.InterfaceType(), " {",
		)
		f.P("return &d.", g.indexDescriptorField(index))
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
	for _, index := range g.Database.Indexes {
		indexDescriptor := IndexDescriptorCodeGenerator{Index: index}
		f.P(g.indexDescriptorField(index), ": ", indexDescriptor.StructType(), "{")
		f.P("indexID: ", strconv.Quote(string(index.Name)), ",")
		for _, column := range index.Columns {
			columnDescriptor := GenericColumnDescriptorCodeGenerator{}
			f.P(g.indexColumnDescriptorField(column), ": ", columnDescriptor.StructType(), "{")
			f.P("columnID: ", strconv.Quote(string(column.Column)), ",")
			// TODO: Resolve reference to the original table column to reference more metadata.
			f.P("},")
		}
		f.P("},")
	}
	f.P("}")
}

func (g DatabaseDescriptorCodeGenerator) tableDescriptorField(table *spanddl.Table) string {
	return strcase.LowerCamelCase(string(table.Name))
}

func (g DatabaseDescriptorCodeGenerator) indexDescriptorField(index *spanddl.Index) string {
	return strcase.LowerCamelCase(string(index.Name))
}

func (g DatabaseDescriptorCodeGenerator) indexColumnDescriptorField(field spansql.KeyPart) string {
	return strcase.LowerCamelCase(string(field.Column))
}

func (g DatabaseDescriptorCodeGenerator) columnDescriptorField(column *spanddl.Column) string {
	return strcase.LowerCamelCase(string(column.Name))
}

func (g DatabaseDescriptorCodeGenerator) globalVariableName() string {
	return "descriptor"
}
