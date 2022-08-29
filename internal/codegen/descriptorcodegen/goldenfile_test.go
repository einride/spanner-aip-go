package descriptorcodegen

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/spanner-aip/internal/codegen"
	"go.einride.tech/spanner-aip/spanddl"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/golden"
)

func runGoldenFileTest(t *testing.T, name string, fn func(*spanddl.Database, *codegen.File)) {
	t.Helper()
	testdataFiles, err := filepath.Glob("testdata/*.sql")
	assert.NilError(t, err)
	for _, testdataFile := range testdataFiles {
		testdataFile := testdataFile
		t.Run(fmt.Sprintf("%s/%s", name, testdataFile), func(t *testing.T) {
			t.Parallel()
			testdata, err := ioutil.ReadFile(testdataFile)
			assert.NilError(t, err)
			ddl, err := spansql.ParseDDL(testdataFile, string(testdata))
			assert.NilError(t, err)
			var db spanddl.Database
			assert.NilError(t, db.ApplyDDL(ddl))
			goldenFile := testdataFile + "." + name + ".go"
			buildTag := "testdata." + filepath.Base(testdataFile) + "." + name
			f := codegen.NewFile(codegen.FileConfig{
				Filename:    goldenFile,
				Package:     "testdata",
				GeneratedBy: t.Name(),
				BuildTag:    buildTag,
			})
			fn(&db, f)
			actual, err := f.Content()
			assert.NilError(t, err)
			golden.Assert(t, string(actual), filepath.Base(goldenFile))
			var stderr strings.Builder
			goBuildCommand := exec.Command("go", "build", "-tags="+buildTag, goldenFile) //nolint: gosec
			goBuildCommand.Stderr = &stderr
			assert.NilError(t, goBuildCommand.Run(), stderr.String())
		})
	}
}
