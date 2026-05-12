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

type transpileOptions struct {
	enumValuesAsStrings bool
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
	if op == spansql.Or {
		return t.transpileOrCallExpr(e)
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

func isHasWildcard(e *expr.Expr) bool {
	sv, ok := e.GetConstExpr().GetConstantKind().(*expr.Constant_StringValue)
	return ok && sv.StringValue == "*"
}

// transpileOrCallExpr collapses chains of `col = X OR col = Y OR …` on the
// same column into a single `col IN UNNEST(@param)` predicate. This keeps
// the resulting spansql tree shallow and side-steps Spanner's 75-deep limit
// on nested boolean function calls. Leaves that don't match the foldable
// `column = constant` shape are passed through unchanged.
func (t *Transpiler) transpileOrCallExpr(e *expr.Expr) (spansql.BoolExpr, error) {
	leaves := flattenOr(e)
	type bucket struct {
		colExpr    *expr.Expr
		kind       valueKind
		values     []interface{}
		origLeaves []*expr.Expr
	}
	buckets := map[string]*bucket{}
	type slot struct {
		bucketKey string
		leaf      *expr.Expr
	}
	var slots []slot
	seen := map[string]struct{}{}
	for _, leaf := range leaves {
		key, k, v, colExpr, ok := t.classifyEqLeaf(leaf)
		if !ok {
			slots = append(slots, slot{leaf: leaf})
			continue
		}
		b, exists := buckets[key]
		if !exists {
			b = &bucket{colExpr: colExpr, kind: k}
			buckets[key] = b
		} else if b.kind != k {
			slots = append(slots, slot{leaf: leaf})
			continue
		}
		b.values = append(b.values, v)
		b.origLeaves = append(b.origLeaves, leaf)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			slots = append(slots, slot{bucketKey: key})
		}
	}
	disjuncts := make([]spansql.BoolExpr, 0, len(slots))
	for _, s := range slots {
		if s.bucketKey != "" {
			b := buckets[s.bucketKey]
			if len(b.values) >= 2 {
				col, err := t.transpileExpr(b.colExpr)
				if err != nil {
					return nil, err
				}
				arr, err := t.arrayParam(b.values, b.kind)
				if err != nil {
					return nil, err
				}
				disjuncts = append(disjuncts, spansql.InOp{
					Unnest: true,
					LHS:    col,
					RHS:    []spansql.Expr{arr},
				})
				continue
			}
			tr, err := t.transpileExpr(b.origLeaves[0])
			if err != nil {
				return nil, err
			}
			be, ok := tr.(spansql.BoolExpr)
			if !ok {
				return nil, fmt.Errorf("unexpected argument to `%s`: not a bool expr", filtering.FunctionOr)
			}
			disjuncts = append(disjuncts, be)
			continue
		}
		tr, err := t.transpileExpr(s.leaf)
		if err != nil {
			return nil, err
		}
		be, ok := tr.(spansql.BoolExpr)
		if !ok {
			return nil, fmt.Errorf("unexpected argument to `%s`: not a bool expr", filtering.FunctionOr)
		}
		disjuncts = append(disjuncts, be)
	}
	// Raw InOp disjuncts are emitted without parens; transpileExpr wraps
	// CallExpr results in Paren, but the OR fold short-circuits that path.
	// When joining multiple disjuncts, wrap each InOp explicitly so the
	// output style matches ((a = @p0) OR (b = @p1)).
	if len(disjuncts) > 1 {
		for i := range disjuncts {
			if _, isIn := disjuncts[i].(spansql.InOp); isIn {
				disjuncts[i] = spansql.Paren{Expr: disjuncts[i]}
			}
		}
	}
	return joinOr(disjuncts), nil
}

// classifyEqLeaf reports whether leaf is a foldable `column = constant`
// expression. When true it returns the column-path key, the value kind, the
// extracted scalar value, and the column subexpression.
func (t *Transpiler) classifyEqLeaf(leaf *expr.Expr) (string, valueKind, interface{}, *expr.Expr, bool) {
	call := leaf.GetCallExpr()
	if call == nil || call.GetFunction() != filtering.FunctionEquals || len(call.GetArgs()) != 2 {
		return "", 0, nil, nil, false
	}
	if t.isSubstringMatchExpr(leaf) {
		return "", 0, nil, nil, false
	}
	lhs := call.GetArgs()[0]
	key, ok := columnPathKey(lhs)
	if !ok {
		return "", 0, nil, nil, false
	}
	v, kind, ok := t.extractEqRHSValue(call.GetArgs()[1])
	if !ok {
		return "", 0, nil, nil, false
	}
	return key, kind, v, lhs, true
}

// flattenOr walks an OR-tree depth-first and returns its leaves in source
// order. A leaf is any node whose call expression is not filtering.FunctionOr.
func flattenOr(e *expr.Expr) []*expr.Expr {
	var leaves []*expr.Expr
	var walk func(*expr.Expr)
	walk = func(n *expr.Expr) {
		call := n.GetCallExpr()
		if call != nil && call.GetFunction() == filtering.FunctionOr && len(call.GetArgs()) == 2 {
			walk(call.GetArgs()[0])
			walk(call.GetArgs()[1])
			return
		}
		leaves = append(leaves, n)
	}
	walk(e)
	return leaves
}

// columnPathKey returns a stable dotted string key for a column path
// expression (chained SelectExpr over IdentExpr, or plain IdentExpr) and
// ok=true. For any other shape it returns ok=false.
func columnPathKey(e *expr.Expr) (string, bool) {
	if ident := e.GetIdentExpr(); ident != nil {
		return ident.GetName(), true
	}
	if sel := e.GetSelectExpr(); sel != nil {
		parent, ok := columnPathKey(sel.GetOperand())
		if !ok {
			return "", false
		}
		return parent + "." + sel.GetField(), true
	}
	return "", false
}

// extractEqRHSValue returns the scalar Go value (and its kind) that the
// given RHS of an = comparison would bind. It handles ConstExpr literals
// and enum IdentExpr values, mirroring the enum-resolution branch of
// transpileIdentExpr. Returns ok=false for anything else.
func (t *Transpiler) extractEqRHSValue(rhs *expr.Expr) (interface{}, valueKind, bool) {
	if c := rhs.GetConstExpr(); c != nil {
		switch v := c.GetConstantKind().(type) {
		case *expr.Constant_StringValue:
			return v.StringValue, kindString, true
		case *expr.Constant_Int64Value:
			return v.Int64Value, kindInt64, true
		case *expr.Constant_Uint64Value:
			return int64(v.Uint64Value), kindInt64, true
		case *expr.Constant_DoubleValue:
			return v.DoubleValue, kindDouble, true
		case *expr.Constant_BoolValue:
			return v.BoolValue, kindBool, true
		}
		return nil, 0, false
	}
	ident := rhs.GetIdentExpr()
	if ident == nil {
		return nil, 0, false
	}
	rhsType, ok := t.filter.CheckedExpr.GetTypeMap()[rhs.GetId()]
	if !ok {
		return nil, 0, false
	}
	msgName := rhsType.GetMessageType()
	if msgName == "" {
		return nil, 0, false
	}
	enumType, err := protoregistry.GlobalTypes.FindEnumByName(protoreflect.FullName(msgName))
	if err != nil {
		return nil, 0, false
	}
	enumValue := enumType.Descriptor().Values().ByName(protoreflect.Name(ident.GetName()))
	if enumValue == nil {
		return nil, 0, false
	}
	if t.options.enumValuesAsStrings {
		return string(enumValue.Name()), kindString, true
	}
	return int64(enumValue.Number()), kindInt64, true
}

// arrayParam binds a homogeneous slice of values as a single ARRAY param
// and returns its reference. The kind determines the concrete slice type.
func (t *Transpiler) arrayParam(values []interface{}, kind valueKind) (spansql.Param, error) {
	switch kind {
	case kindString:
		s := make([]string, len(values))
		for i, v := range values {
			s[i] = v.(string)
		}
		return t.param(s), nil
	case kindInt64:
		s := make([]int64, len(values))
		for i, v := range values {
			s[i] = v.(int64)
		}
		return t.param(s), nil
	case kindDouble:
		s := make([]float64, len(values))
		for i, v := range values {
			s[i] = v.(float64)
		}
		return t.param(s), nil
	case kindBool:
		s := make([]bool, len(values))
		for i, v := range values {
			s[i] = v.(bool)
		}
		return t.param(s), nil
	}
	return "", fmt.Errorf("arrayParam: unsupported value kind %d", kind)
}

// joinOr reduces a slice of disjuncts into a left-skewed LogicalOp{Or}
// chain. A single element is returned unwrapped.
func joinOr(disjuncts []spansql.BoolExpr) spansql.BoolExpr {
	result := disjuncts[0]
	for i := 1; i < len(disjuncts); i++ {
		result = spansql.LogicalOp{
			Op:  spansql.Or,
			LHS: result,
			RHS: disjuncts[i],
		}
	}
	return result
}

type valueKind int

const (
	kindString valueKind = iota + 1
	kindInt64
	kindDouble
	kindBool
)

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
	// String: wildcard checks presence (non-null and non-empty, i.e. not the proto default).
	case identType.GetPrimitive() == expr.Type_STRING:
		if !isHasWildcard(constExpr) {
			return nil, fmt.Errorf("unsupported: HAS operator on string only supports wildcard (:*)")
		}
		col, err := t.transpileIdentExpr(identExpr)
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
		col, err := t.transpileIdentExpr(identExpr)
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
	// Arg 0: column identifier
	identExpr := args[0].GetIdentExpr()
	if identExpr == nil {
		return nil, fmt.Errorf("first argument to %s must be an identifier", callExpr.GetFunction())
	}
	tokenColumn := spansql.ID(identExpr.GetName())
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
