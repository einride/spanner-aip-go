package spanfiltering

import (
	"fmt"
	"time"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type Transpiler struct {
	filter filtering.Filter
}

func (t *Transpiler) Init(filter filtering.Filter) {
	*t = Transpiler{
		filter: filter,
	}
}

func (t *Transpiler) Transpile() (spansql.BoolExpr, error) {
	if t.filter.CheckedExpr == nil {
		return spansql.True, nil
	}
	resultExpr, err := t.transpileExpr(t.filter.CheckedExpr.Expr)
	if err != nil {
		return nil, err
	}
	resultBoolExpr, ok := resultExpr.(spansql.BoolExpr)
	if !ok {
		return nil, fmt.Errorf("not a bool expr")
	}
	return resultBoolExpr, nil
}

func (t *Transpiler) transpileExpr(e *expr.Expr) (spansql.Expr, error) {
	switch e.ExprKind.(type) {
	case *expr.Expr_CallExpr:
		result, err := t.transpileCallExpr(e)
		if err != nil {
			return nil, err
		}
		return spansql.Paren{Expr: result}, nil
	case *expr.Expr_IdentExpr:
		return t.transpileIdentExpr(e)
	case *expr.Expr_SelectExpr:
		return t.transpileSelectExpr(e)
	case *expr.Expr_ConstExpr:
		return t.transpileConstExpr(e)
	default:
		return nil, fmt.Errorf("unsupported expr: %v", e)
	}
}

func (t *Transpiler) transpileConstExpr(e *expr.Expr) (spansql.Expr, error) {
	switch kind := e.GetConstExpr().ConstantKind.(type) {
	case *expr.Constant_StringValue:
		return spansql.StringLiteral(kind.StringValue), nil
	case *expr.Constant_Int64Value:
		return spansql.IntegerLiteral(kind.Int64Value), nil
	default:
		return nil, fmt.Errorf("unsupported const expr: %v", kind)
	}
}

func (t *Transpiler) transpileCallExpr(e *expr.Expr) (spansql.Expr, error) {
	switch e.GetCallExpr().Function {
	case filtering.FunctionHas:
		return t.transpileHasCallExpr(e)
	case filtering.FunctionEquals:
		return t.transpileComparisonCallExpr(e, spansql.Eq)
	case filtering.FunctionLessThan:
		return t.transpileComparisonCallExpr(e, spansql.Lt)
	case filtering.FunctionLessEquals:
		return t.transpileComparisonCallExpr(e, spansql.Le)
	case filtering.FunctionGreaterThan:
		return t.transpileComparisonCallExpr(e, spansql.Gt)
	case filtering.FunctionGreaterEquals:
		return t.transpileComparisonCallExpr(e, spansql.Ge)
	case filtering.FunctionAnd:
		return t.transpileBinaryLogicalCallExpr(e, spansql.And)
	case filtering.FunctionOr:
		return t.transpileBinaryLogicalCallExpr(e, spansql.Or)
	case filtering.FunctionNot:
		return t.transpileNotCallExpr(e)
	case filtering.FunctionTimestamp:
		return t.transpileTimestampCallExpr(e)
	default:
		return nil, fmt.Errorf("unsupported function call: %s", e.GetCallExpr().Function)
	}
}

func (t *Transpiler) transpileIdentExpr(e *expr.Expr) (spansql.Expr, error) {
	identExpr := e.GetIdentExpr()
	identType, ok := t.filter.CheckedExpr.TypeMap[e.Id]
	if !ok {
		return nil, fmt.Errorf("unknown type of ident expr %d", e.Id)
	}
	if messageType := identType.GetMessageType(); messageType != "" {
		if enumType, err := protoregistry.GlobalTypes.FindEnumByName(protoreflect.FullName(messageType)); err == nil {
			if enumValue := enumType.Descriptor().Values().ByName(protoreflect.Name(identExpr.Name)); enumValue != nil {
				// TODO: Configurable support for string literals.
				return spansql.IntegerLiteral(enumValue.Number()), nil
			}
		}
	}
	return spansql.ID(identExpr.Name), nil
}

func (t *Transpiler) transpileSelectExpr(e *expr.Expr) (spansql.Expr, error) {
	selectExpr := e.GetSelectExpr()
	operand, err := t.transpileExpr(selectExpr.Operand)
	if err != nil {
		return nil, err
	}
	switch operand := operand.(type) {
	case spansql.PathExp:
		return append(operand, spansql.ID(selectExpr.Field)), nil
	case spansql.ID:
		return spansql.PathExp{operand, spansql.ID(selectExpr.Field)}, nil
	default:
		return nil, fmt.Errorf("unsupported select expr operand")
	}
}

func (t *Transpiler) transpileNotCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.Args) != 1 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to `%s` expression: %d",
			filtering.FunctionNot,
			len(callExpr.Args),
		)
	}
	rhsExpr, err := t.transpileExpr(callExpr.Args[0])
	if err != nil {
		return nil, err
	}
	rhsBoolExpr, ok := rhsExpr.(spansql.BoolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected argument to `%s`: not a bool expr", filtering.FunctionNot)
	}
	return spansql.LogicalOp{
		Op:  spansql.Not,
		RHS: rhsBoolExpr,
	}, nil
}

func (t *Transpiler) transpileComparisonCallExpr(
	e *expr.Expr,
	op spansql.ComparisonOperator,
) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.Args) != 2 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to `%s`: %d",
			callExpr.GetFunction(),
			len(callExpr.Args),
		)
	}
	lhsExpr, err := t.transpileExpr(callExpr.Args[0])
	if err != nil {
		return nil, err
	}
	rhsExpr, err := t.transpileExpr(callExpr.Args[1])
	if err != nil {
		return nil, err
	}
	return spansql.ComparisonOp{
		Op:  op,
		LHS: lhsExpr,
		RHS: rhsExpr,
	}, nil
}

func (t *Transpiler) transpileBinaryLogicalCallExpr(
	e *expr.Expr,
	op spansql.LogicalOperator,
) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.Args) != 2 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to `%s`: %d",
			callExpr.GetFunction(),
			len(callExpr.Args),
		)
	}
	lhsExpr, err := t.transpileExpr(callExpr.Args[0])
	if err != nil {
		return nil, err
	}
	rhsExpr, err := t.transpileExpr(callExpr.Args[1])
	if err != nil {
		return nil, err
	}
	lhsBoolExpr, ok := lhsExpr.(spansql.BoolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected arguments to `%s`: lhs not a bool expr", callExpr.GetFunction())
	}
	rhsBoolExpr, ok := rhsExpr.(spansql.BoolExpr)
	if !ok {
		return nil, fmt.Errorf("unexpected arguments to `%s` rhs not a bool expr", callExpr.GetFunction())
	}
	return spansql.LogicalOp{
		Op:  op,
		LHS: lhsBoolExpr,
		RHS: rhsBoolExpr,
	}, nil
}

func (t *Transpiler) transpileHasCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.Args) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments to `in` expression: %d", len(callExpr.Args))
	}
	return nil, fmt.Errorf("TODO: add support for transpiling `:`")
}

func (t *Transpiler) transpileTimestampCallExpr(e *expr.Expr) (spansql.TimestampLiteral, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.Args) != 1 {
		return spansql.TimestampLiteral{}, fmt.Errorf(
			"unexpected number of arguments to `%s`: %d", callExpr.Function, len(callExpr.Args),
		)
	}
	arg, err := t.transpileExpr(callExpr.Args[0])
	if err != nil {
		return spansql.TimestampLiteral{}, err
	}
	stringArg, ok := arg.(spansql.StringLiteral)
	if !ok {
		return spansql.TimestampLiteral{}, fmt.Errorf("expected string arg to %s", callExpr.Function)
	}
	timeArg, err := time.Parse(time.RFC3339, string(stringArg))
	if err != nil {
		return spansql.TimestampLiteral{}, fmt.Errorf("invalid string arg to %s: %w", callExpr.Function, err)
	}
	return spansql.TimestampLiteral(timeArg), nil
}
