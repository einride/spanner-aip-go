package typescodegen

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip-spanner/spanddl"
)

func GoType(column *spanddl.Column) reflect.Type {
	switch column.Type.Base {
	case spansql.Bool:
		switch {
		case column.Type.Array:
			return reflect.TypeOf([]spanner.NullBool(nil))
		case column.NotNull:
			return reflect.TypeOf(true)
		default:
			return reflect.TypeOf(spanner.NullBool{})
		}
	case spansql.Int64:
		switch {
		case column.Type.Array:
			return reflect.TypeOf([]spanner.NullInt64(nil))
		case column.NotNull:
			return reflect.TypeOf(int64(0))
		default:
			return reflect.TypeOf(spanner.NullInt64{})
		}
	case spansql.Float64:
		switch {
		case column.Type.Array:
			return reflect.TypeOf([]spanner.NullFloat64(nil))
		case column.NotNull:
			return reflect.TypeOf(float64(0))
		default:
			return reflect.TypeOf(spanner.NullFloat64{})
		}
	case spansql.String:
		switch {
		case column.Type.Array:
			return reflect.TypeOf([]spanner.NullString(nil))
		case column.NotNull:
			return reflect.TypeOf("")
		default:
			return reflect.TypeOf(spanner.NullString{})
		}
	case spansql.Bytes:
		switch {
		case column.Type.Array:
			return reflect.TypeOf([][]byte(nil))
		default:
			return reflect.TypeOf([]byte(nil))
		}
	case spansql.Date:
		switch {
		case column.Type.Array:
			return reflect.TypeOf([]spanner.NullDate(nil))
		case column.NotNull:
			return reflect.TypeOf(civil.Date{})
		default:
			return reflect.TypeOf(spanner.NullDate{})
		}
	case spansql.Timestamp:
		switch {
		case column.Type.Array:
			return reflect.TypeOf([]spanner.NullDate(nil))
		case column.NotNull:
			return reflect.TypeOf(civil.Date{})
		default:
			return reflect.TypeOf(spanner.NullDate{})
		}
	default:
		panic(fmt.Sprintf("unhandled base type: %v", column.Type.Base))
	}
}
