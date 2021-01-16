package spanddl

import (
	"fmt"

	"cloud.google.com/go/spanner/spansql"
)

// Table represents a Spanner table.
type Table struct {
	Name              spansql.ID
	Columns           []*Column
	InterleavedTables []*Table
	PrimaryKey        []spansql.KeyPart
	Interleave        *spansql.Interleave
}

func (t *Table) applyAlterTable(stmt *spansql.AlterTable) error {
	switch alteration := stmt.Alteration.(type) {
	case spansql.AddColumn:
		return t.applyAddColumnAlteration(alteration)
	case spansql.AlterColumn:
		return t.applyAlterColumnAlteration(alteration)
	case spansql.DropColumn:
		return t.applyDropColumnAlteration(alteration)
	case spansql.AddConstraint:
		return t.applyAddConstraintAlteration(alteration)
	case spansql.DropConstraint:
		return t.applyDropConstraintAlteration(alteration)
	case spansql.SetOnDelete:
		return t.applySetOnDeleteAlteration(alteration)
	default:
		return fmt.Errorf("unhandled alteration (%s)", alteration.SQL())
	}
}

func (t *Table) Column(name spansql.ID) (*Column, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return nil, false
}

func (t *Table) applyAddColumnAlteration(alteration spansql.AddColumn) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("apply ADD COLUMN: %w", err)
		}
	}()
	if _, ok := t.Column(alteration.Def.Name); ok {
		return fmt.Errorf("column %s already exists", alteration.Def.Name)
	}
	var column Column
	if err := column.applyColumnDef(alteration.Def); err != nil {
		return fmt.Errorf("column %s: %w", alteration.Def.Name, err)
	}
	t.Columns = append(t.Columns, &column)
	return nil
}

func (t *Table) applyAlterColumnAlteration(alteration spansql.AlterColumn) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("apply ALTER COLUMN: %w", err)
		}
	}()
	column, ok := t.Column(alteration.Name)
	if !ok {
		return fmt.Errorf("column %s does not exist", alteration.Name)
	}
	if err := column.applyColumnAlteration(alteration.Alteration); err != nil {
		return fmt.Errorf("column %s: %w", alteration.Name, err)
	}
	return nil
}

func (t *Table) applyDropColumnAlteration(alteration spansql.DropColumn) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("apply DROP COLUMN: %w", err)
		}
	}()
	i := t.indexOfColumn(alteration.Name)
	if i == -1 {
		return fmt.Errorf("column %s does not exist", alteration.Name)
	}
	t.Columns = append(t.Columns[:i], t.Columns[i+1:]...)
	return nil
}

func (t *Table) applyAddConstraintAlteration(_ spansql.AddConstraint) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("apply ADD CONSTRAINT: %w", err)
		}
	}()
	return fmt.Errorf("TDOO: implement me")
}

func (t *Table) applyDropConstraintAlteration(_ spansql.DropConstraint) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("apply DROP CONSTRAINT: %w", err)
		}
	}()
	return fmt.Errorf("TDOO: implement me")
}

func (t *Table) applySetOnDeleteAlteration(alteration spansql.SetOnDelete) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("apply SET ON DELETE: %w", err)
		}
	}()
	if t.Interleave == nil {
		return fmt.Errorf("table is not interleaved")
	}
	t.Interleave.OnDelete = alteration.Action
	return nil
}

func (t *Table) indexOfColumn(name spansql.ID) int {
	for i, column := range t.Columns {
		if column.Name == name {
			return i
		}
	}
	return -1
}
