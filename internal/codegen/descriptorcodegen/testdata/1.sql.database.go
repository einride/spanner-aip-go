// Code generated by TestDatabaseDescriptorCodeGenerator_GenerateCode/database/testdata/1.sql. DO NOT EDIT.
//go:build testdata.1.sql.database
// +build testdata.1.sql.database

package testdata

import (
	"cloud.google.com/go/spanner/spansql"
)

func Descriptor() DatabaseDescriptor {
	return &descriptor
}

var descriptor = databaseDescriptor{
	singers: singersTableDescriptor{
		tableID: "Singers",
		singerId: columnDescriptor{
			columnID:             "SingerId",
			columnType:           spansql.Type{Array: false, Base: 1, Len: 0, ProtoRef: ""},
			notNull:              true,
			allowCommitTimestamp: false,
		},
		firstName: columnDescriptor{
			columnID:             "FirstName",
			columnType:           spansql.Type{Array: false, Base: 4, Len: 1024, ProtoRef: ""},
			notNull:              false,
			allowCommitTimestamp: false,
		},
		lastName: columnDescriptor{
			columnID:             "LastName",
			columnType:           spansql.Type{Array: false, Base: 4, Len: 1024, ProtoRef: ""},
			notNull:              false,
			allowCommitTimestamp: false,
		},
		singerInfo: columnDescriptor{
			columnID:             "SingerInfo",
			columnType:           spansql.Type{Array: false, Base: 5, Len: 9223372036854775807, ProtoRef: ""},
			notNull:              false,
			allowCommitTimestamp: false,
		},
	},
}

type DatabaseDescriptor interface {
	Singers() SingersTableDescriptor
}

type databaseDescriptor struct {
	singers singersTableDescriptor
}

func (d *databaseDescriptor) Singers() SingersTableDescriptor {
	return &d.singers
}

type SingersTableDescriptor interface {
	TableName() string
	TableID() spansql.ID
	ColumnNames() []string
	ColumnIDs() []spansql.ID
	ColumnExprs() []spansql.Expr
	SingerId() ColumnDescriptor
	FirstName() ColumnDescriptor
	LastName() ColumnDescriptor
	SingerInfo() ColumnDescriptor
}

type singersTableDescriptor struct {
	tableID    spansql.ID
	singerId   columnDescriptor
	firstName  columnDescriptor
	lastName   columnDescriptor
	singerInfo columnDescriptor
}

func (d *singersTableDescriptor) TableName() string {
	return string(d.tableID)
}

func (d *singersTableDescriptor) TableID() spansql.ID {
	return d.tableID
}

func (d *singersTableDescriptor) ColumnNames() []string {
	return []string{
		"SingerId",
		"FirstName",
		"LastName",
		"SingerInfo",
	}
}

func (d *singersTableDescriptor) ColumnIDs() []spansql.ID {
	return []spansql.ID{
		"SingerId",
		"FirstName",
		"LastName",
		"SingerInfo",
	}
}

func (d *singersTableDescriptor) ColumnExprs() []spansql.Expr {
	return []spansql.Expr{
		spansql.ID("SingerId"),
		spansql.ID("FirstName"),
		spansql.ID("LastName"),
		spansql.ID("SingerInfo"),
	}
}

func (d *singersTableDescriptor) SingerId() ColumnDescriptor {
	return &d.singerId
}

func (d *singersTableDescriptor) FirstName() ColumnDescriptor {
	return &d.firstName
}

func (d *singersTableDescriptor) LastName() ColumnDescriptor {
	return &d.lastName
}

func (d *singersTableDescriptor) SingerInfo() ColumnDescriptor {
	return &d.singerInfo
}

type ColumnDescriptor interface {
	ColumnID() spansql.ID
	ColumnName() string
	ColumnType() spansql.Type
	NotNull() bool
	AllowCommitTimestamp() bool
}

type columnDescriptor struct {
	columnID             spansql.ID
	columnType           spansql.Type
	notNull              bool
	allowCommitTimestamp bool
}

func (d *columnDescriptor) ColumnName() string {
	return string(d.columnID)
}

func (d *columnDescriptor) ColumnID() spansql.ID {
	return d.columnID
}

func (d *columnDescriptor) ColumnType() spansql.Type {
	return d.columnType
}

func (d *columnDescriptor) ColumnExpr() spansql.Expr {
	return d.columnID
}

func (d *columnDescriptor) NotNull() bool {
	return d.notNull
}

func (d *columnDescriptor) AllowCommitTimestamp() bool {
	return d.allowCommitTimestamp
}
