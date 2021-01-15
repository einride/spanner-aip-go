package databasecodegen

import (
	"github.com/stoewer/go-strcase"
	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

type KeyRangeCodeGenerator struct {
	Table *spanddl.Table
}

func (g KeyRangeCodeGenerator) Type() string {
	return strcase.UpperCamelCase(string(g.Table.Name)) + "KeyRange"
}

func (g KeyRangeCodeGenerator) GenerateCode(f *codegen.File) {
	spannerPkg := f.Import("cloud.google.com/go/spanner")
	partialKey := KeyPrefixCodeGenerator(g)
	f.P()
	f.P("type ", g.Type(), " struct {")
	f.P("Start ", partialKey.Type())
	f.P("End ", partialKey.Type())
	f.P("Kind ", spannerPkg, ".KeyRangeKind")
	f.P("}")
	f.P()
	f.P("func (k ", g.Type(), ") SpannerKeySet() ", spannerPkg, ".KeySet {")
	f.P("return ", spannerPkg, ".KeyRange{")
	f.P("Start: k.Start.SpannerKey(),")
	f.P("End: k.End.SpannerKey(),")
	f.P("Kind: k.Kind,")
	f.P("}")
	f.P("}")
}
