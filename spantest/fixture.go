package spantest

import (
	"testing"

	"cloud.google.com/go/spanner"
)

// Fixture is a Spanner test fixture.
type Fixture interface {
	// NewDatabaseFromDLLFiles creates a new database and applies the DDL files from the provided glob.
	NewDatabaseFromDDLFiles(t testing.TB, globs ...string) *spanner.Client
	// NewDatabaseFromStatements creates a new database and applies the provided DLL statements.
	NewDatabaseFromStatements(t testing.TB, statements []string) *spanner.Client
}
