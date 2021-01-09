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
