package spanddl

import (
	"fmt"

	"cloud.google.com/go/spanner/spansql"
)

// Column represents a Spanner table column.
type Column struct {
	Name    spansql.ID
	Type    spansql.Type
	NotNull bool
	Options spansql.ColumnOptions
}

func (c *Column) applyColumnDef(def spansql.ColumnDef) error {
	c.Name = def.Name
	c.Type = def.Type
	c.NotNull = def.NotNull
	c.Options = def.Options
	return nil
}

func (c *Column) applyColumnAlteration(alteration spansql.ColumnAlteration) error {
	switch alteration := alteration.(type) {
	case spansql.SetColumnOptions:
		return c.applySetColumnOptionsAlteration(alteration)
	case spansql.SetColumnType:
		return c.applySetColumnTypeAlteration(alteration)
	default:
		return fmt.Errorf("unhandled column alteration (%s)", alteration.SQL())
	}
}

func (c *Column) applySetColumnOptionsAlteration(alteration spansql.SetColumnOptions) (err error) {
	c.Options = alteration.Options
	return nil
}

func (c *Column) applySetColumnTypeAlteration(alteration spansql.SetColumnType) (err error) {
	c.Type = alteration.Type
	c.NotNull = alteration.NotNull
	return nil
}
