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

const (
	tokenColumnSuffix          = "_tokens"
	minSearchNgramsQueryLength = 2
)

// WithSearchNgrams enables SEARCH_NGRAMS support with the given filter function name.
// The function name must match the name used in DeclareSearchNgramsFunction.
func WithSearchNgrams(filterFunctionName string) TranspileOption {
	return func(options *transpileOptions) {
		options.searchNgramsFunctionName = filterFunctionName
	}
}

// DeclareSearchNgramsFunction declares a SEARCH_NGRAMS function for use in filter expressions.
// It declares two overloads:
//   - 2-arg: functionName(column, query) — required params only
//   - 5-arg: functionName(column, query, language_tag, min_ngrams, min_ngrams_percent) — all params
func DeclareSearchNgramsFunction(functionName string) filtering.DeclarationOption {
	return filtering.DeclareFunction(
		functionName,
		filtering.NewFunctionOverload(
			functionName+"_2",
			filtering.TypeBool,
			filtering.TypeString, filtering.TypeString,
		),
		filtering.NewFunctionOverload(
			functionName+"_5",
			filtering.TypeBool,
			filtering.TypeString, filtering.TypeString, filtering.TypeString, filtering.TypeInt, filtering.TypeFloat,
		),
	)
}

type transpileOptions struct {
	enumValuesAsStrings      bool
	searchNgramsFunctionName string
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
	default:
		fnName := e.GetCallExpr().GetFunction()
		if t.options.searchNgramsFunctionName != "" && fnName == t.options.searchNgramsFunctionName {
			return t.transpileSearchNgramsCallExpr(e)
		}
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
	return spansql.ID(identExpr.GetName()), nil
}

func (t *Transpiler) transpileSelectExpr(e *expr.Expr) (spansql.Expr, error) {
	selectExpr := e.GetSelectExpr()
	operand, err := t.transpileExpr(selectExpr.GetOperand())
	if err != nil {
		return nil, err
	}
	switch operand := operand.(type) {
	case spansql.PathExp:
		return append(operand, spansql.ID(selectExpr.GetField())), nil
	case spansql.ID:
		return spansql.PathExp{operand, spansql.ID(selectExpr.GetField())}, nil
	default:
		return nil, fmt.Errorf("unsupported select expr operand")
	}
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
	if lhs.GetIdentExpr() == nil {
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
	return spansql.ComparisonOp{
		Op:  spansql.Like,
		LHS: spansql.ID(lhs.GetIdentExpr().GetName()),
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
	if len(callExpr.GetArgs()) != 2 {
		return nil, fmt.Errorf("unexpected number of arguments to `in` expression: %d", len(callExpr.GetArgs()))
	}
	identExpr := callExpr.GetArgs()[0]
	constExpr := callExpr.GetArgs()[1]
	if identExpr.GetIdentExpr() == nil {
		return nil, fmt.Errorf("TODO: add support for transpiling `:` where LHS is other than Ident")
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
	// Arg 0: column identifier → append _tokens suffix.
	identExpr := args[0].GetIdentExpr()
	if identExpr == nil {
		return nil, fmt.Errorf("first argument to %s must be an identifier", callExpr.GetFunction())
	}
	tokenColumn := spansql.ID(identExpr.GetName() + tokenColumnSuffix)
	// Arg 1: ngrams_query string, must be at least 2 characters.
	queryConst := args[1].GetConstExpr()
	if queryConst == nil {
		return nil, fmt.Errorf("second argument to %s must be a string constant", callExpr.GetFunction())
	}
	queryString, ok := queryConst.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return nil, fmt.Errorf("second argument to %s must be a string constant", callExpr.GetFunction())
	}
	if len(queryString.StringValue) < minSearchNgramsQueryLength {
		return nil, fmt.Errorf(
			"search query for %s must be at least %d characters, got %d",
			callExpr.GetFunction(), minSearchNgramsQueryLength, len(queryString.StringValue),
		)
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
