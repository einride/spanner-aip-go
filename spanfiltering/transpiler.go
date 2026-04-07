package spanfiltering

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// FunctionSearchNgrams is the function name for SEARCH_NGRAMS in filter expressions.
const FunctionSearchNgrams = "searchNgrams"

// DeclareSearchNgramsFunction declares the searchNgrams function for use in filter expressions.
// It declares two overloads:
//   - 2-arg: searchNgrams(column, query) — required params only
//   - 5-arg: searchNgrams(column, query, language_tag, min_ngrams, min_ngrams_percent) — all params
func DeclareSearchNgramsFunction() filtering.DeclarationOption {
	return filtering.DeclareFunction(
		FunctionSearchNgrams,
		filtering.NewFunctionOverload(
			FunctionSearchNgrams+"_2",
			filtering.TypeBool,
			filtering.TypeString, filtering.TypeString,
		),
		filtering.NewFunctionOverload(
			FunctionSearchNgrams+"_5",
			filtering.TypeBool,
			filtering.TypeString, filtering.TypeString, filtering.TypeString, filtering.TypeInt, filtering.TypeFloat,
		),
	)
}

type Transpiler struct {
	filter       filtering.Filter
	params       map[string]interface{}
	paramCounter int
	options      transpileOptions
}

type TranspileOption func(options *transpileOptions)

func WithEnumValuesAsStrings() TranspileOption {
	return func(options *transpileOptions) {
		options.enumValuesAsStrings = true
	}
}

// WithTableAlias qualifies top-level column references with alias (e.g. author -> t.author).
// If the path already begins with the same alias (as in a filter written as t.author), it is not doubled.
func WithTableAlias(alias string) TranspileOption {
	return func(options *transpileOptions) {
		options.tableAlias = alias
	}
}

type transpileOptions struct {
	enumValuesAsStrings bool
	tableAlias          string
}

func (t *Transpiler) Init(filter filtering.Filter, options ...TranspileOption) {
	*t = Transpiler{
		filter: filter,
		params: make(map[string]interface{}),
	}
	for _, option := range options {
		option(&t.options)
	}
}

func (t *Transpiler) Transpile() (spansql.BoolExpr, map[string]interface{}, error) {
	if t.filter.CheckedExpr == nil {
		return spansql.True, nil, nil
	}
	resultExpr, err := t.transpileExpr(t.filter.CheckedExpr.GetExpr())
	if err != nil {
		return nil, nil, err
	}
	resultBoolExpr, err := asBoolExpr(resultExpr)
	if err != nil {
		return nil, nil, fmt.Errorf("not a bool expr")
	}
	params := t.params
	if t.paramCounter == 0 {
		params = nil
	}
	return resultBoolExpr, params, nil
}

func (t *Transpiler) transpileExpr(e *expr.Expr) (spansql.Expr, error) {
	switch e.GetExprKind().(type) {
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
	switch kind := e.GetConstExpr().GetConstantKind().(type) {
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
	switch e.GetCallExpr().GetFunction() {
	case filtering.FunctionHas:
		return t.transpileHasCallExpr(e)
	case filtering.FunctionEquals:
		if t.isSubstringMatchExpr(e) {
			return t.transpileSubstringMatchExpr(e)
		}
		return t.transpileComparisonCallExpr(e, spansql.Eq)
	case filtering.FunctionNotEquals:
		return t.transpileComparisonCallExpr(e, spansql.Ne)
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
	case FunctionSearchNgrams:
		return t.transpileSearchNgramsCallExpr(e)
	default:
		return nil, fmt.Errorf("unsupported function call: %s", e.GetCallExpr().GetFunction())
	}
}

func (t *Transpiler) transpileIdentExpr(e *expr.Expr) (spansql.Expr, error) {
	identExpr := e.GetIdentExpr()
	identType, ok := t.filter.CheckedExpr.GetTypeMap()[e.GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of ident expr %d", e.GetId())
	}
	if messageType := identType.GetMessageType(); messageType != "" {
		if enumType, err := protoregistry.GlobalTypes.FindEnumByName(protoreflect.FullName(messageType)); err == nil {
			if enumValue := enumType.Descriptor().Values().ByName(protoreflect.Name(identExpr.GetName())); enumValue != nil {
				if t.options.enumValuesAsStrings {
					return t.param(string(enumValue.Name())), nil
				}
				// spanner does not support int32
				return t.param(int64(enumValue.Number())), nil
			}
		}
	}
	return t.qualifyColumnPath(spansql.ID(identExpr.GetName())), nil
}

// asBoolExpr accepts column refs for boolean contexts. PathExp is a valid SQL boolean
// column reference but does not implement spansql.BoolExpr; wrap it in Paren.
func asBoolExpr(e spansql.Expr) (spansql.BoolExpr, error) {
	if be, ok := e.(spansql.BoolExpr); ok {
		return be, nil
	}
	if pe, ok := e.(spansql.PathExp); ok {
		return spansql.Paren{Expr: pe}, nil
	}
	return nil, fmt.Errorf("not a bool expr")
}

// qualifyColumnPath qualifies a column path with a table alias.
// If the path already begins with the same alias (as in a filter written as t.author), it is not doubled.
func (t *Transpiler) qualifyColumnPath(e spansql.Expr) spansql.Expr {
	if t.options.tableAlias == "" {
		return e
	}

	var path spansql.PathExp
	switch x := e.(type) {
	case spansql.ID:
		path = spansql.PathExp{x}
	case spansql.PathExp:
		path = x
	default:
		return e
	}

	if len(path) == 0 {
		return e
	}
	if string(path[0]) == t.options.tableAlias {
		return e
	}

	out := make(spansql.PathExp, len(path)+1)
	out[0] = spansql.ID(t.options.tableAlias)
	copy(out[1:], path)
	return out
}

func (t *Transpiler) transpileSelectExpr(e *expr.Expr) (spansql.Expr, error) {
	selectExpr := e.GetSelectExpr()
	operand, err := t.transpileExpr(selectExpr.GetOperand())
	if err != nil {
		return nil, err
	}
	var path spansql.PathExp
	switch operandType := operand.(type) {
	case spansql.PathExp:
		operandType = append(operandType, spansql.ID(selectExpr.GetField()))
		path = operandType
	case spansql.ID:
		path = spansql.PathExp{operandType, spansql.ID(selectExpr.GetField())}
	default:
		return nil, fmt.Errorf("unsupported select expr operand")
	}
	return t.qualifyColumnPath(path), nil
}

func (t *Transpiler) transpileNotCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 1 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to `%s` expression: %d",
			filtering.FunctionNot,
			len(callExpr.GetArgs()),
		)
	}
	rhsExpr, err := t.transpileExpr(callExpr.GetArgs()[0])
	if err != nil {
		return nil, err
	}
	rhsBoolExpr, err := asBoolExpr(rhsExpr)
	if err != nil {
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
	if len(callExpr.GetArgs()) != 2 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to `%s`: %d",
			callExpr.GetFunction(),
			len(callExpr.GetArgs()),
		)
	}
	lhsExpr, err := t.transpileExpr(callExpr.GetArgs()[0])
	if err != nil {
		return nil, err
	}
	rhsExpr, err := t.transpileExpr(callExpr.GetArgs()[1])
	if err != nil {
		return nil, err
	}
	return spansql.ComparisonOp{
		Op:  op,
		LHS: lhsExpr,
		RHS: rhsExpr,
	}, nil
}

func (t *Transpiler) isSubstringMatchExpr(
	e *expr.Expr,
) bool {
	if len(e.GetCallExpr().GetArgs()) != 2 {
		return false
	}
	lhs := e.GetCallExpr().GetArgs()[0]
	if lhs.GetIdentExpr() == nil && lhs.GetSelectExpr() == nil {
		return false
	}
	rhs := e.GetCallExpr().GetArgs()[1]
	if rhs.GetConstExpr() == nil {
		return false
	}
	rhsStringExpr, ok := rhs.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return false
	}
	return strings.HasPrefix(rhsStringExpr.StringValue, "*") || strings.HasSuffix(rhsStringExpr.StringValue, "*")
}

func (t *Transpiler) transpileSubstringMatchExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	lhs := e.GetCallExpr().GetArgs()[0]
	rhs := e.GetCallExpr().GetArgs()[1]
	rhsString := rhs.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue).StringValue
	if strings.Contains(strings.TrimSuffix(strings.TrimPrefix(rhsString, "*"), "*"), "*") {
		return nil, fmt.Errorf(
			"unsupported argument to `%s`: wildcard only supported in leading or trailing positions",
			e.GetCallExpr().GetFunction(),
		)
	}
	lhsExpr, err := t.transpileExpr(lhs)
	if err != nil {
		return nil, err
	}
	return spansql.ComparisonOp{
		Op:  spansql.Like,
		LHS: lhsExpr,
		RHS: t.param(strings.ReplaceAll(rhsString, "*", "%")),
	}, nil
}

func (t *Transpiler) transpileBinaryLogicalCallExpr(
	e *expr.Expr,
	op spansql.LogicalOperator,
) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 2 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to `%s`: %d",
			callExpr.GetFunction(),
			len(callExpr.GetArgs()),
		)
	}
	lhsExpr, err := t.transpileExpr(callExpr.GetArgs()[0])
	if err != nil {
		return nil, err
	}
	rhsExpr, err := t.transpileExpr(callExpr.GetArgs()[1])
	if err != nil {
		return nil, err
	}
	lhsBoolExpr, err := asBoolExpr(lhsExpr)
	if err != nil {
		return nil, fmt.Errorf("unexpected arguments to `%s`: lhs not a bool expr", callExpr.GetFunction())
	}
	rhsBoolExpr, err := asBoolExpr(rhsExpr)
	if err != nil {
		return nil, fmt.Errorf("unexpected arguments to `%s` rhs not a bool expr", callExpr.GetFunction())
	}
	return spansql.LogicalOp{
		Op:  op,
		LHS: lhsBoolExpr,
		RHS: rhsBoolExpr,
	}, nil
}

func isHasWildcard(e *expr.Expr) bool {
	sv, ok := e.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
	return ok && sv.StringValue == "*"
}

func (t *Transpiler) transpileHasCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments to `in` expression: %d", len(callExpr.GetArgs()))
	}
	lhsExprNode := callExpr.GetArgs()[0]
	constExpr := callExpr.GetArgs()[1]
	if lhsExprNode.GetIdentExpr() == nil && lhsExprNode.GetSelectExpr() == nil {
		return nil, fmt.Errorf("TODO: add support for transpiling `:` where LHS is other than Ident or Select")
	}
	if constExpr.GetConstExpr() == nil {
		return nil, fmt.Errorf("TODO: add support for transpiling `:` where RHS is other than Const")
	}
	identType, ok := t.filter.CheckedExpr.GetTypeMap()[callExpr.GetArgs()[0].GetId()]
	if !ok {
		return nil, fmt.Errorf("unknown type of ident expr %d", e.GetId())
	}
	switch {
	// Repeated primitives:
	// > Repeated fields query to see if the repeated structure contains a matching element.
	case identType.GetListType().GetElemType().GetPrimitive() != expr.Type_PRIMITIVE_TYPE_UNSPECIFIED:
		iden, err := t.transpileExpr(lhsExprNode)
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
	// String: wildcard checks presence (non-null and non-empty, i.e. not the proto default).
	case identType.GetPrimitive() == expr.Type_STRING:
		if !isHasWildcard(constExpr) {
			return nil, fmt.Errorf("unsupported: HAS operator on string only supports wildcard (:*)")
		}
		col, err := t.transpileExpr(lhsExprNode)
		if err != nil {
			return nil, err
		}
		return spansql.LogicalOp{
			Op:  spansql.And,
			LHS: spansql.IsOp{LHS: col, Neg: true, RHS: spansql.Null},
			RHS: spansql.ComparisonOp{Op: spansql.Ne, LHS: col, RHS: t.param("")},
		}, nil
	// Timestamp: wildcard checks presence (non-null and not the proto default).
	// The proto default for google.protobuf.Timestamp is UTC Epoch (seconds: 0, nanos: 0).
	case identType.GetWellKnown() == expr.Type_TIMESTAMP:
		if !isHasWildcard(constExpr) {
			return nil, fmt.Errorf("unsupported: HAS operator on timestamp only supports wildcard (:*)")
		}
		col, err := t.transpileExpr(lhsExprNode)
		if err != nil {
			return nil, err
		}
		return spansql.LogicalOp{
			Op:  spansql.And,
			LHS: spansql.IsOp{LHS: col, Neg: true, RHS: spansql.Null},
			RHS: spansql.ComparisonOp{Op: spansql.Ne, LHS: col, RHS: t.param(time.Unix(0, 0).UTC())},
		}, nil
	default:
		return nil, fmt.Errorf(
			"TODO: add support for transpiling `:` on other types than repeated primitives, strings and timestamps",
		)
	}
}

func (t *Transpiler) transpileTimestampCallExpr(e *expr.Expr) (spansql.Expr, error) {
	callExpr := e.GetCallExpr()
	if len(callExpr.GetArgs()) != 1 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to `%s`: %d", callExpr.GetFunction(), len(callExpr.GetArgs()),
		)
	}
	constArg, ok := callExpr.GetArgs()[0].GetExprKind().(*expr.Expr_ConstExpr)
	if !ok {
		return nil, fmt.Errorf("expected constant string arg to %s", callExpr.GetFunction())
	}
	stringArg, ok := constArg.ConstExpr.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return nil, fmt.Errorf("expected constant string arg to %s", callExpr.GetFunction())
	}
	timeArg, err := time.Parse(time.RFC3339, stringArg.StringValue)
	if err != nil {
		return nil, fmt.Errorf("invalid string arg to %s: %w", callExpr.GetFunction(), err)
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

func (t *Transpiler) transpileSearchNgramsCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	callExpr := e.GetCallExpr()
	args := callExpr.GetArgs()
	if len(args) != 2 && len(args) != 5 {
		return nil, fmt.Errorf(
			"unexpected number of arguments to %s: %d (expected 2 or 5)",
			callExpr.GetFunction(), len(args),
		)
	}
	tokenColumn, err := t.transpileExpr(args[0])
	if err != nil {
		return nil, err
	}
	// Arg 1: ngrams_query string, must be at least 2 characters.
	queryConst := args[1].GetConstExpr()
	if queryConst == nil {
		return nil, fmt.Errorf("second argument to %s must be a string constant", callExpr.GetFunction())
	}
	_, ok := queryConst.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return nil, fmt.Errorf("second argument to %s must be a string constant", callExpr.GetFunction())
	}
	queryParam, err := t.transpileConstExpr(args[1])
	if err != nil {
		return nil, err
	}
	sqlArgs := []spansql.Expr{tokenColumn, queryParam}
	// 5-arg form: optional named parameters.
	if len(args) == 5 {
		// Arg 2: language_tag (string, skip if empty).
		langConst, ok := args[2].GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
		if ok && langConst.StringValue != "" {
			langParam, err := t.transpileConstExpr(args[2])
			if err != nil {
				return nil, err
			}
			sqlArgs = append(sqlArgs, spansql.DefinitionExpr{
				Key: "language_tag", Value: langParam,
			})
		}
		// Arg 3: min_ngrams (int64, skip if zero).
		minConst, ok := args[3].GetConstExpr().GetConstantKind().(*expr.Constant_Int64Value)
		if ok && minConst.Int64Value != 0 {
			minParam, err := t.transpileConstExpr(args[3])
			if err != nil {
				return nil, err
			}
			sqlArgs = append(sqlArgs, spansql.DefinitionExpr{
				Key: "min_ngrams", Value: minParam,
			})
		}
		// Arg 4: min_ngrams_percent (float64, skip if zero).
		pctConst, ok := args[4].GetConstExpr().GetConstantKind().(*expr.Constant_DoubleValue)
		if ok && pctConst.DoubleValue != 0 {
			pctParam, err := t.transpileConstExpr(args[4])
			if err != nil {
				return nil, err
			}
			sqlArgs = append(sqlArgs, spansql.DefinitionExpr{
				Key: "min_ngrams_percent", Value: pctParam,
			})
		}
	}
	return spansql.Func{Name: "SEARCH_NGRAMS", Args: sqlArgs}, nil
}
