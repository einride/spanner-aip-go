package spanfiltering

import (
	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip/filtering"
)

// TranspileFilter transpiles a parsed AIP filter expression to a spansql.BoolExpr, and
// parameters used in the expression.
// The parameter map is nil if the expression does not contain any parameters.
// The returned expression is safe to embed in a larger clause (e.g. as an
// operand of spansql.LogicalOp, or spliced into `WHERE base AND %s`): a
// top-level AND/OR is parenthesized, and every other expression kind binds
// tighter than AND and OR.
func TranspileFilter(
	filter filtering.Filter,
	options ...TranspileOption,
) (spansql.BoolExpr, map[string]interface{}, error) {
	var t Transpiler
	t.Init(filter, options...)
	return t.Transpile()
}
