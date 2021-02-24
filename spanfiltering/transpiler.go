package spanfiltering

import (
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type Transpiler struct {
	filter       filtering.Filter
	params       map[string]interface{}
	paramCounter int
}

func (t *Transpiler) Init(filter filtering.Filter) {
	*t = Transpiler{
		filter: filter,
		params: make(map[string]interface{}),
	}
}

func (t *Transpiler) Transpile() (spansql.BoolExpr, map[string]interface{}, error) {
	if t.filter.CheckedExpr == nil {
		return spansql.True, nil, nil
	}
	resultExpr, err := t.transpileExpr(t.filter.CheckedExpr.Expr)
	if err != nil {
		return nil, nil, err
	}
	resultBoolExpr, ok := resultExpr.(spansql.BoolExpr)
	if !ok {
		return nil, nil, fmt.Errorf("not a bool expr")
	}
	params := t.params
	if t.paramCounter == 0 {
		params = nil
	}
	return resultBoolExpr, params, nil
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
	case *expr.Constant_BoolValue:
		return t.param(kind.BoolValue), nil
	case *expr.Constant_DoubleValue:
		return t.param(kind.DoubleValue), nil
	case *expr.Constant_Int64Value:
		return t.param(kind.Int64Value), nil
	case *expr.Constant_StringValue:
		return t.param(kind.StringValue), nil
	case *expr.Constant_Uint64Value:
		// spanner does not support uint64
		return t.param(int64(kind.Uint64Value)), nil
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
				// spanner does not support int32
				return t.param(int64(enumValue.Number())), nil
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
	identExpr := callExpr.Args[0]
	constExpr := callExpr.Args[1]
	if identExpr.GetIdentExpr() == nil {
		return nil, fmt.Errorf("TODO: add support for transpiling `:` where LHS is other than Ident")
	}
	if constExpr.GetConstExpr() == nil {
		return nil, fmt.Errorf("TODO: add support for transpiling `:` where RHS is other than Const")
	}
	identType, ok := t.filter.CheckedExpr.TypeMap[callExpr.Args[0].Id]
	if !ok {
		return nil, fmt.Errorf("unknown type of ident expr %d", e.Id)
	}
	switch {
	// Repeated primitives:
	// > Repeated fields query to see if the repeated structure contains a matching element.
	case identType.GetListType().GetElemType().GetPrimitive() != expr.Type_PRIMITIVE_TYPE_UNSPECIFIED:
		iden, err := t.transpileIdentExpr(identExpr)
		if err != nil {
			return nil, err
		}
		con, err := t.transpileConstExpr(constExpr)
		if err != nil {
			return nil, err
		}
		return spansql.InOp{
			Unnest: true,
			LHS:    con,
			RHS:    []spansql.Expr{iden},
		}, nil
	default:
		return nil, fmt.Errorf("TODO: add support for transpiling `:` on other types than repeated primitives")
	}
}

func (t *Transpiler) transpileTimestampCallExpr(e *expr.Expr) (spansql.Expr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.Args) != 1 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to `%s`: %d", callExpr.Function, len(callExpr.Args),
		)
	}
	constArg, ok := callExpr.Args[0].ExprKind.(*expr.Expr_ConstExpr)
	if !ok {
		return nil, fmt.Errorf("expected constant string arg to %s", callExpr.Function)
	}
	stringArg, ok := constArg.ConstExpr.ConstantKind.(*expr.Constant_StringValue)
	if !ok {
		return nil, fmt.Errorf("expected constant string arg to %s", callExpr.Function)
	}
	timeArg, err := time.Parse(time.RFC3339, stringArg.StringValue)
	if err != nil {
		return nil, fmt.Errorf("invalid string arg to %s: %w", callExpr.Function, err)
	}
	return t.param(timeArg), nil
}

func (t *Transpiler) param(param interface{}) spansql.Param {
	p := t.nextParam()
	t.params[p] = param
	return spansql.Param(p)
}

func (t *Transpiler) nextParam() string {
	param := "param_" + strconv.Itoa(t.paramCounter)
	t.paramCounter++
	return param
}
