package spanddl

import (
	"fmt"
	"strings"

	"cloud.google.com/go/spanner/spansql"
)

// Database represents an in-memory Spanner database.
type Database struct {
	// Tables in the database.
	Tables []*Table
	// Indexes in the database.
	Indexes []*Index
}

// Table looks up a table with the provided name.
func (d *Database) Table(name spansql.ID) (*Table, bool) {
	for _, table := range d.Tables {
		if table.Name == name {
			return table, true
		}
	}
	return nil, false
}

// Index looks up an index with the provided name.
func (d *Database) Index(name spansql.ID) (*Index, bool) {
	for _, index := range d.Indexes {
		if index.Name == name {
			return index, true
		}
	}
	return nil, false
}

// ApplyDDL applies the provided DDL statement to the database.
func (d *Database) ApplyDDL(ddl *spansql.DDL) error {
	for _, stmt := range ddl.List {
		if err := d.applyDDLStmt(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (d *Database) applyDDLStmt(stmt spansql.DDLStmt) error {
	switch stmt := stmt.(type) {
	case *spansql.CreateTable:
		return d.applyCreateTable(stmt)
	case *spansql.AlterTable:
		return d.applyAlterTable(stmt)
	case *spansql.DropTable:
		return d.applyDropTable(stmt)
	case *spansql.CreateIndex:
		return d.applyCreateIndex(stmt)
	case *spansql.DropIndex:
		return d.applyDropIndex(stmt)
	default:
		return fmt.Errorf("unsupported DDL statement: (%s)", stmt.SQL())
	}
}

func (d *Database) applyCreateTable(stmt *spansql.CreateTable) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("CREATE TABLE: %w", err)
		}
	}()
	if _, ok := d.Table(stmt.Name); ok {
		return fmt.Errorf("table %s already exists", stmt.Name)
	}
	table := &Table{
		Name:              stmt.Name,
		Columns:           make([]*Column, 0, len(stmt.Columns)),
		Interleave:        stmt.Interleave,
		PrimaryKey:        stmt.PrimaryKey,
		RowDeletionPolicy: stmt.RowDeletionPolicy,
	}
	for _, columnDef := range stmt.Columns {
		var column Column
		if err := column.applyColumnDef(columnDef); err != nil {
			return err
		}
		table.Columns = append(table.Columns, &column)
	}
	if table.Interleave != nil {
		parent, ok := d.Table(table.Interleave.Parent)
		if !ok {
			return fmt.Errorf("parent table %s does not exist", table.Interleave.Parent)
		}
		parent.InterleavedTables = append(parent.InterleavedTables, table)
	}
	d.Tables = append(d.Tables, table)
	return nil
}

func (d *Database) applyAlterTable(stmt *spansql.AlterTable) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("apply ALTER TABLE: %w", err)
		}
	}()
	table, ok := d.Table(stmt.Name)
	if !ok {
		return fmt.Errorf("table %s does not exist", stmt.Name)
	}
	return table.applyAlterTable(stmt)
}

func (d *Database) applyDropTable(stmt *spansql.DropTable) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("DROP TABLE: %w", err)
		}
	}()
	i := d.indexOfTable(stmt.Name)
	if i == -1 {
		return fmt.Errorf("table %s does not exist", stmt.Name)
	}
	if table, ok := d.Table(stmt.Name); ok && len(table.InterleavedTables) > 0 {
		names := make([]string, 0, len(table.InterleavedTables))
		for _, t := range table.InterleavedTables {
			names = append(names, string(t.Name))
		}
		return fmt.Errorf("table %s has interleaved tables %s", stmt.Name, strings.Join(names, ", "))
	}
	d.Tables = append(d.Tables[:i], d.Tables[i+1:]...)
	return nil
}

func (d *Database) applyCreateIndex(stmt *spansql.CreateIndex) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("CREATE INDEX: %w", err)
		}
	}()
	if _, ok := d.Index(stmt.Name); ok {
		return fmt.Errorf("index %s already exists", stmt.Name)
	}
	if _, ok := d.Table(stmt.Table); !ok {
		return fmt.Errorf("table %s does not exist", stmt.Table)
	}
	d.Indexes = append(d.Indexes, &Index{
		Name:         stmt.Name,
		Table:        stmt.Table,
		Columns:      stmt.Columns,
		Unique:       stmt.Unique,
		NullFiltered: stmt.NullFiltered,
		Storing:      stmt.Storing,
		Interleave:   stmt.Interleave,
	})
	return nil
}

func (d *Database) applyDropIndex(stmt *spansql.DropIndex) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("DROP INDEX: %w", err)
		}
	}()
	i := d.indexOfIndex(stmt.Name)
	if i == -1 {
		return fmt.Errorf("index %s does not exist", stmt.Name)
	}
	d.Indexes = append(d.Indexes[:i], d.Indexes[i+1:]...)
	return nil
}

func (d *Database) indexOfTable(name spansql.ID) int {
	for i, table := range d.Tables {
		if table.Name == name {
			return i
		}
	}
	return -1
}

func (d *Database) indexOfIndex(name spansql.ID) int {
	for i, index := range d.Indexes {
		if index.Name == name {
			return i
		}
	}
	return -1
}
