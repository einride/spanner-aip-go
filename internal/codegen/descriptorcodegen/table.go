package descriptorcodegen

import (
	"strconv"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func generateTableDescriptorInterface(f *codegen.File, table *spanddl.Table) {
	f.P()
	f.P("type ", typeOfTableDescriptorInterface(table), " interface {")
	f.P("TableName() string")
	f.P("TableID() spansql.ID")
	f.P("ColumnNames() []string")
	f.P("ColumnIDs() []spansql.ID")
	for _, column := range table.Columns {
		f.P(nameOfColumnDescriptor(column), "() ColumnDescriptor")
	}
	f.P("}")
}

func generateGenericColumnDescriptorInterface(f *codegen.File) {
	f.P()
	f.P("type ColumnDescriptor interface {")
	f.P("ColumnID() spansql.ID")
	f.P("ColumnName() string")
	f.P("ColumnType() spansql.Type")
	f.P("NotNull() bool")
	f.P("Options() spansql.ColumnOptions")
	f.P("}")
}

func generateTableDescriptorStruct(f *codegen.File, table *spanddl.Table) {
	f.P()
	f.P("type ", typeOfTableDescriptorStruct(table), " struct {")
	f.P("tableID spansql.ID")
	for _, column := range table.Columns {
		f.P(private(nameOfColumnDescriptor(column)), " ", typeOfColumnDescriptorStruct(table, column))
	}
	f.P("}")
	f.P()
	f.P("func (d *", typeOfTableDescriptorStruct(table), ") ", "TableName() string {")
	f.P("return string(d.tableID)")
	f.P("}")
	f.P()
	f.P("func (d *", typeOfTableDescriptorStruct(table), ") ", "TableID() spansql.ID {")
	f.P("return d.tableID")
	f.P("}")
	f.P()
	f.P("func (d *", typeOfTableDescriptorStruct(table), ") ", "ColumnNames() []string {")
	f.P("return []string{")
	for _, column := range table.Columns {
		f.P(strconv.Quote(string(column.Name)), ",")
	}
	f.P("}")
	f.P("}")
	f.P()
	f.P("func (d *", typeOfTableDescriptorStruct(table), ") ", "ColumnIDs() []spansql.ID {")
	f.P("return []spansql.ID{")
	for _, column := range table.Columns {
		f.P(strconv.Quote(string(column.Name)), ",")
	}
	f.P("}")
	f.P("}")
	for _, column := range table.Columns {
		f.P()
		f.P(
			"func (d *", typeOfTableDescriptorStruct(table), ") ",
			nameOfColumnDescriptor(column), "() ColumnDescriptor",
			" {",
		)
		f.P("return &d.", private(nameOfColumnDescriptor(column)))
		f.P("}")
	}
}

func generateColumnDescriptorStruct(f *codegen.File, table *spanddl.Table, column *spanddl.Column) {
	f.P()
	f.P("type ", typeOfColumnDescriptorStruct(table, column), " struct {")
	f.P("columnID spansql.ID")
	f.P("columnType spansql.Type")
	f.P("notNull bool")
	f.P("options spansql.ColumnOptions")
	f.P("}")
	f.P()
	f.P("func (d *", typeOfColumnDescriptorStruct(table, column), ") ", "ColumnName() string {")
	f.P("return string(d.columnID)")
	f.P("}")
	f.P()
	f.P("func (d *", typeOfColumnDescriptorStruct(table, column), ") ", "ColumnID() spansql.ID {")
	f.P("return d.columnID")
	f.P("}")
	f.P()
	f.P("func (d *", typeOfColumnDescriptorStruct(table, column), ") ", "ColumnType() spansql.Type {")
	f.P("return d.columnType")
	f.P("}")
	f.P()
	f.P("func (d *", typeOfColumnDescriptorStruct(table, column), ") ", "NotNull() bool {")
	f.P("return d.notNull")
	f.P("}")
	f.P()
	f.P("func (d *", typeOfColumnDescriptorStruct(table, column), ") ", "Options() spansql.ColumnOptions {")
	f.P("return d.options")
	f.P("}")
}
