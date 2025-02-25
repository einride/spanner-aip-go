package spantest

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/spanner/spansql"
	"gotest.tools/v3/assert"
)

type databaseCreationConfig struct {
	statements            []string
	protoDescriptorReader io.Reader
}

type DatabaseCreationOption func(t testing.TB, cfg *databaseCreationConfig)

func FromStatements(statements []string) DatabaseCreationOption {
	return func(_ testing.TB, dcc *databaseCreationConfig) {
		dcc.statements = statements
	}
}

func FromGlobs(globs ...string) DatabaseCreationOption {
	return func(t testing.TB, dcc *databaseCreationConfig) {
		var files []string
		for _, glob := range globs {
			globFiles, err := filepath.Glob(glob)
			assert.NilError(t, err)
			files = append(files, globFiles...)
		}
		for _, file := range files {
			content, err := os.ReadFile(file)
			assert.NilError(t, err)
			ddl, err := spansql.ParseDDL(file, string(content))
			assert.NilError(t, err)
			for _, ddlStmt := range ddl.List {
				dcc.statements = append(dcc.statements, ddlStmt.SQL())
			}
		}
		assert.Assert(t, len(dcc.statements) > 0)
	}
}

func WithProtoDescriptor(reader io.Reader) DatabaseCreationOption {
	return func(_ testing.TB, dcc *databaseCreationConfig) {
		dcc.protoDescriptorReader = reader
	}
}

func getDatabaseCreationConfig(t testing.TB, options ...DatabaseCreationOption) databaseCreationConfig {
	var cfg databaseCreationConfig
	for _, option := range options {
		option(t, &cfg)
	}
	if cfg.statements == nil {
		t.Fatal("no statements")
		return databaseCreationConfig{}
	}
	return cfg
}
