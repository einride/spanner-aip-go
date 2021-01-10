package descriptorcodegen

import (
	"github.com/stoewer/go-strcase"
	"go.einride.tech/aip-spanner/spanddl"
)

func typeOfDescriptorInterface() string {
	return "DatabaseDescriptor"
}

func typeOfDescriptorStruct() string {
	return "databaseDescriptor"
}

func nameOfDescriptor() string {
	return "descriptor"
}

func nameOfTableDescriptor(table *spanddl.Table) string {
	return strcase.UpperCamelCase(string(table.Name))
}

func nameOfColumnDescriptor(column *spanddl.Column) string {
	return strcase.UpperCamelCase(string(column.Name))
}

func typeOfColumnDescriptorInterface(table *spanddl.Table, column *spanddl.Column) string {
	return strcase.UpperCamelCase(string(table.Name)) + strcase.UpperCamelCase(string(column.Name)) + "ColumnDescriptor"
}

func typeOfColumnDescriptorStruct(table *spanddl.Table, column *spanddl.Column) string {
	return private(typeOfColumnDescriptorInterface(table, column))
}

func typeOfTableDescriptorInterface(table *spanddl.Table) string {
	return strcase.UpperCamelCase(string(table.Name)) + "TableDescriptor"
}

func typeOfTableDescriptorStruct(table *spanddl.Table) string {
	return private(typeOfTableDescriptorInterface(table))
}

func private(s string) string {
	return strcase.LowerCamelCase(s)
}
