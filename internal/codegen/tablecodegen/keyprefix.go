package tablecodegen

import (
	"fmt"
	"reflect"
	"strconv"

	"cloud.google.com/go/spanner/spansql"
	"github.com/stoewer/go-strcase"
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/internal/codegen/typescodegen"
	"go.einride.tech/aip-spanner/spanddl"
)

type KeyPrefixCodeGenerator struct {
	Table *spanddl.Table
}

func (g KeyPrefixCodeGenerator) Type() string {
	return strcase.UpperCamelCase(string(g.Table.Name)) + "KeyPrefix"
}

func (g KeyPrefixCodeGenerator) FieldName(keyPart spansql.KeyPart) string {
	return strcase.UpperCamelCase(string(keyPart.Column))
}

func (g KeyPrefixCodeGenerator) GenerateCode(f *codegen.File) {
	if len(g.Table.PrimaryKey) == 0 {
		return
	}
	f.P()
	f.P("type ", g.Type(), " struct {")
	f.P(g.FieldName(g.Table.PrimaryKey[0]), " ", g.columnType(f, g.Table.PrimaryKey[0]))
	for _, keyPart := range g.Table.PrimaryKey[1:] {
		f.P(g.FieldName(keyPart), " ", g.columnType(f, keyPart))
		f.P("Valid", g.FieldName(keyPart), " bool")
	}
	f.P("}")
	g.generateSpannerKeyMethod(f)
	g.generateDeleteMethod(f)
	g.generateBoolExprMethod(f)
	g.generateQualifiedBoolExprMethod(f)
}

func (g KeyPrefixCodeGenerator) generateSpannerKeyMethod(f *codegen.File) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (k ", g.Type(), ") SpannerKey() ", spannerPkg, ".Key {")
	if len(g.Table.PrimaryKey) == 1 {
		f.P("return ", spannerPkg, ".Key{k.", g.FieldName(g.Table.PrimaryKey[0]), "}")
		f.P("}")
		return
	}
	f.P("n := 1")
	for _, keyPart := range g.Table.PrimaryKey[1:] {
		f.P("if k.Valid", g.FieldName(keyPart), " {")
		f.P("n++")
	}
	for range g.Table.PrimaryKey[1:] {
		f.P("}")
	}
	f.P("result := make(", spannerPkg, ".Key, 0, n)")
	f.P("result = append(result, k.", g.FieldName(g.Table.PrimaryKey[0]), ")")
	for _, keyPart := range g.Table.PrimaryKey[1:] {
		f.P("if k.Valid", g.FieldName(keyPart), " {")
		f.P("result = append(result, k.", g.FieldName(keyPart), ")")
	}
	for range g.Table.PrimaryKey[1:] {
		f.P("}")
	}
	f.P("return result")
	f.P("}")
	f.P()
	f.P("func (k ", g.Type(), ") SpannerKeySet() ", spannerPkg, ".KeySet {")
	f.P("return k.SpannerKey()")
	f.P("}")
}

func (g KeyPrefixCodeGenerator) generateDeleteMethod(f *codegen.File) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	f.P()
	f.P("func (k ", g.Type(), ") Delete() *", spannerPkg, ".Mutation {")
	f.P("return ", spannerPkg, ".Delete(", strconv.Quote(string(g.Table.Name)), ", k.SpannerKey())")
	f.P("}")
}

func (g KeyPrefixCodeGenerator) generateBoolExprMethod(f *codegen.File) {
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	k0 := g.Table.PrimaryKey[0]
	f.P("func (k ", g.Type(), ") BoolExpr() ", spansqlPkg, ".BoolExpr {")
	f.P("b := ", spansqlPkg, ".BoolExpr(", spansqlPkg, ".ComparisonOp{")
	f.P("Op: ", spansqlPkg, ".Eq,")
	f.P("LHS: ", spansqlPkg, ".ID(", strconv.Quote(string(k0.Column)), "),")
	f.P(
		"RHS: ", g.columnSpanSQLType(f, k0),
		"(k.", g.FieldName(k0), typescodegen.ValueAccessor(g.keyColumn(k0)), "),",
	)
	f.P("})")
	for _, keyPart := range g.Table.PrimaryKey[1:] {
		f.P("if k.Valid", g.FieldName(keyPart), " {")
		f.P("b = ", spansqlPkg, ".LogicalOp{")
		f.P("Op: ", spansqlPkg, ".And,")
		f.P("LHS: b,")
		f.P("RHS: ", spansqlPkg, ".ComparisonOp{")
		f.P("Op: ", spansqlPkg, ".Eq,")
		f.P("LHS: ", spansqlPkg, ".ID(", strconv.Quote(string(keyPart.Column)), "),")
		f.P(
			"RHS: ", g.columnSpanSQLType(f, keyPart),
			"(k.", g.FieldName(keyPart), typescodegen.ValueAccessor(g.keyColumn(keyPart)), "),",
		)
		f.P("},")
		f.P("}")
	}
	for range g.Table.PrimaryKey[1:] {
		f.P("}")
	}
	f.P("return ", spansqlPkg, ".Paren{Expr: b}")
	f.P("}")
}

func (g KeyPrefixCodeGenerator) generateQualifiedBoolExprMethod(f *codegen.File) {
	spansqlPkg := f.Import("cloud.google.com/go/spanner/spansql")
	f.P()
	k0 := g.Table.PrimaryKey[0]
	f.P("func (k ", g.Type(), ") QualifiedBoolExpr(prefix ", spansqlPkg, ".PathExp) ", spansqlPkg, ".BoolExpr {")
	f.P("b := ", spansqlPkg, ".BoolExpr(", spansqlPkg, ".ComparisonOp{")
	f.P("Op: ", spansqlPkg, ".Eq,")
	f.P("LHS: append(prefix, ", spansqlPkg, ".ID(", strconv.Quote(string(g.Table.PrimaryKey[0].Column)), ")),")
	f.P(
		"RHS: ", g.columnSpanSQLType(f, k0),
		"(k.", g.FieldName(k0), typescodegen.ValueAccessor(g.keyColumn(k0)), "),",
	)
	f.P("})")
	for _, keyPart := range g.Table.PrimaryKey[1:] {
		f.P("if k.Valid", g.FieldName(keyPart), " {")
		f.P("b = ", spansqlPkg, ".LogicalOp{")
		f.P("Op: ", spansqlPkg, ".And,")
		f.P("LHS: b,")
		f.P("RHS: ", spansqlPkg, ".ComparisonOp{")
		f.P("Op: ", spansqlPkg, ".Eq,")
		f.P("LHS: append(prefix, ", spansqlPkg, ".ID(", strconv.Quote(string(keyPart.Column)), ")),")
		f.P(
			"RHS: ", g.columnSpanSQLType(f, keyPart),
			"(k.", g.FieldName(keyPart), typescodegen.ValueAccessor(g.keyColumn(keyPart)), "),",
		)
		f.P("},")
		f.P("}")
	}
	for range g.Table.PrimaryKey[1:] {
		f.P("}")
	}
	f.P("return ", spansqlPkg, ".Paren{Expr: b}")
	f.P("}")
}

func (g KeyPrefixCodeGenerator) keyColumn(keyPart spansql.KeyPart) *spanddl.Column {
	column, ok := g.Table.Column(keyPart.Column)
	if !ok {
		panic(fmt.Errorf("table %s has no column %s", g.Table.Name, keyPart.Column))
	}
	return column
}

func (g KeyPrefixCodeGenerator) columnType(f *codegen.File, keyPart spansql.KeyPart) reflect.Type {
	t := typescodegen.GoType(g.keyColumn(keyPart))
	if t.PkgPath() != "" {
		_ = f.Import(t.PkgPath())
	}
	return t
}

func (g KeyPrefixCodeGenerator) columnSpanSQLType(f *codegen.File, keyPart spansql.KeyPart) reflect.Type {
	t := typescodegen.SpanSQLType(g.keyColumn(keyPart))
	if t.PkgPath() != "" {
		_ = f.Import(t.PkgPath())
	}
	return t
}
