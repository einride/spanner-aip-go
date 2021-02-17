package spanfiltering

import (
	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip/filtering"
)

// TranspileFilter transpiles a parsed AIP filter expression to a spansql.BoolExpr, and
// parameters used in the expression.
// The parameter map is nil if the expression does not contain any parameters.
func TranspileFilter(filter filtering.Filter) (spansql.BoolExpr, map[string]interface{}, error) {
	var t Transpiler
	t.Init(filter)
	return t.Transpile()
}
