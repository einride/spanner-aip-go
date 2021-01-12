package typescodegen

import (
	"fmt"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip-spanner/spanddl"
)

func ValueAccessor(column *spanddl.Column) string {
	if column.NotNull || column.Type.Array {
		return ""
	}
	switch column.Type.Base {
	case spansql.Bool:
		return ".Bool"
	case spansql.Int64:
		return ".Int64"
	case spansql.Float64:
		return ".Float64"
	case spansql.String:
		return ".StringVal"
	case spansql.Bytes:
		return ""
	case spansql.Date:
		return ".Date"
	case spansql.Timestamp:
		return ".Time"
	default:
		panic(fmt.Errorf("unhandled type: %v", column.Type.Base))
	}
}
