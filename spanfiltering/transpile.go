package spanfiltering

import (
	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip/filtering"
)

// TranspileFilter transpiles a parsed AIP filter expression to a spansql.BoolExpr.
func TranspileFilter(filter filtering.Filter) (spansql.BoolExpr, error) {
	var t Transpiler
	t.Init(filter)
	return t.Transpile()
}
