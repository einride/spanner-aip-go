package tablecodegen

import (
	"testing"

	"go.einride.tech/aip-spanner/internal/codegen"
	"go.einride.tech/aip-spanner/spanddl"
)

func TestCommonCodeGenerator_GenerateCode(t *testing.T) {
	t.Parallel()
	runGoldenFileTest(t, "common", func(db *spanddl.Database, f *codegen.File) {
		CommonCodeGenerator{}.GenerateCode(f)
	})
}
