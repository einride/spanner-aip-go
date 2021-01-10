package descriptorcodegen

import (
	"fmt"
	"strconv"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

// GenerateDatabaseDescriptor generates a top-level read-only database descriptor with schema metadata.
func GenerateDatabaseDescriptor(f *codegen.File, db *spanddl.Database) {
	generateImports(f)
	generateDatabaseDescriptorFunction(f)
	generateDatabaseDescriptorValue(f, db)
	generateDatabaseDescriptorInterface(f, db)
	generateDatabaseDescriptorStruct(f, db)
	generateGenericColumnDescriptorInterface(f)
	for _, table := range db.Tables {
		generateTableDescriptorInterface(f, table)
	}
	for _, table := range db.Tables {
		generateTableDescriptorStruct(f, table)
		for _, column := range table.Columns {
			generateColumnDescriptorStruct(f, table, column)
		}
	}
}

func generateImports(f *codegen.File) {
	f.P()
	f.P("import (")
	f.P(`"cloud.google.com/go/spanner/spansql"`)
	f.P(")")
	f.P()
	f.P("var (")
	f.P(`_ = spansql.ID("")`)
	f.P(")")
}

func generateDatabaseDescriptorFunction(f *codegen.File) {
	f.P()
	f.P("func Descriptor() ", typeOfDescriptorInterface(), " {")
	f.P("return &", nameOfDescriptor())
	f.P("}")
}

func generateDatabaseDescriptorInterface(f *codegen.File, db *spanddl.Database) {
	f.P()
	f.P("type ", typeOfDescriptorInterface(), " interface {")
	for _, table := range db.Tables {
		f.P(nameOfTableDescriptor(table), "() ", typeOfTableDescriptorInterface(table))
	}
	f.P("}")
}

func generateDatabaseDescriptorValue(f *codegen.File, db *spanddl.Database) {
	f.P()
	f.P("var ", nameOfDescriptor(), " = ", typeOfDescriptorStruct(), "{")
	for _, table := range db.Tables {
		f.P(private(nameOfTableDescriptor(table)), ": ", typeOfTableDescriptorStruct(table), "{")
		f.P("tableID: ", strconv.Quote(string(table.Name)), ",")
		for _, column := range table.Columns {
			f.P(private(nameOfColumnDescriptor(column)), ": ", typeOfColumnDescriptorStruct(table, column), "{")
			f.P("columnID: ", strconv.Quote(string(column.Name)), ",")
			f.P("columnType: ", fmt.Sprintf("%#v", column.Type), ",")
			f.P("notNull: ", column.NotNull, ",")
			f.P("options: ", fmt.Sprintf("%#v", column.Options), ",")
			f.P("},")
		}
		f.P("},")
	}
	f.P("}")
}

func generateDatabaseDescriptorStruct(f *codegen.File, db *spanddl.Database) {
	f.P()
	f.P("type ", typeOfDescriptorStruct(), " struct {")
	for _, table := range db.Tables {
		f.P(private(nameOfTableDescriptor(table)), " ", typeOfTableDescriptorStruct(table))
	}
	f.P("}")
	for _, table := range db.Tables {
		f.P()
		f.P(
			"func (d *", typeOfDescriptorStruct(), ") ",
			nameOfTableDescriptor(table), "() ", typeOfTableDescriptorInterface(table), " {",
		)
		f.P("return &d.", private(nameOfTableDescriptor(table)))
		f.P("}")
	}
}
