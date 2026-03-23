package spanddl

import "cloud.google.com/go/spanner/spansql"

type Index struct {
	Name         spansql.ID
	Table        spansql.ID
	Columns      []spansql.KeyPart
	Unique       bool
	NullFiltered bool
	Storing      []spansql.ID
	Interleave   spansql.ID
}

type SearchIndex struct {
	Name    spansql.ID
	Table   spansql.ID
	Columns []spansql.KeyPart

	Storing        []spansql.ID
	PartitionBy    []spansql.ID
	OrderBy        []spansql.Order
	WhereIsNotNull []spansql.ID
	Interleave     spansql.ID
	Options        spansql.SearchIndexOptions
}
