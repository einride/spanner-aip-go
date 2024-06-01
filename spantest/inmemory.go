package spantest

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spannertest"
	"cloud.google.com/go/spanner/spansql"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
)

// InMemoryFixture is a test fixture running the Spanner emulator.
type InMemoryFixture struct {
	ctx context.Context
}

// NewInMemoryFixture creates a test fixture for the in-memory Spanner emulator.
func NewInMemoryFixture(t testing.TB) Fixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if tt, ok := t.(*testing.T); ok {
		if deadline, ok := tt.Deadline(); ok {
			ctx, cancel = context.WithDeadline(ctx, deadline)
			t.Cleanup(cancel)
		}
	}
	return &InMemoryFixture{ctx: ctx}
}

// NewDatabaseFromDDLFiles implements Fixture.
func (fx *InMemoryFixture) NewDatabaseFromDDLFiles(t testing.TB, globs ...string) *spanner.Client {
	t.Helper()
	var files []string
	for _, glob := range globs {
		globFiles, err := filepath.Glob(glob)
		assert.NilError(t, err)
		files = append(files, globFiles...)
	}
	var statements []string
	for _, file := range files {
		content, err := os.ReadFile(file)
		assert.NilError(t, err)
		ddl, err := spansql.ParseDDL(file, string(content))
		assert.NilError(t, err)
		for _, ddlStmt := range ddl.List {
			statements = append(statements, ddlStmt.SQL())
		}
	}
	assert.Assert(t, len(statements) > 0)
	return fx.NewDatabaseFromStatements(t, statements)
}

// NewDatabaseFromDDLFiles implements Fixture.
func (fx *InMemoryFixture) NewDatabaseFromStatements(t testing.TB, statements []string) *spanner.Client {
	t.Helper()
	const (
		projectID  = "spanner-aip-go"
		instanceID = "in-memory"
	)
	databaseID := "db" + strconv.Itoa(rand.Int()) //nolint: gosec
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	server, err := spannertest.NewServer("localhost:0")
	assert.NilError(t, err)
	t.Cleanup(server.Close)
	conn, err := grpc.NewClient(server.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NilError(t, err)
	client, err := spanner.NewClient(fx.ctx, databaseName, option.WithGRPCConn(conn))
	assert.NilError(t, err)
	t.Cleanup(client.Close)
	for i, statement := range statements {
		ddl, err := spansql.ParseDDL(fmt.Sprintf("statement%d", i), statement)
		assert.NilError(t, err)
		removeUnsupportedStatements(ddl)
		assert.NilError(t, server.UpdateDDL(ddl))
	}
	return client
}

// removeUnsupportedStatements removes any statements which are not yet supported by
// the spannertest package for the purpose of not preventing new spanner
// features from breaking existing tests.
func removeUnsupportedStatements(ddl *spansql.DDL) {
	stmts := make([]spansql.DDLStmt, 0, len(ddl.List))
	for _, stmt := range ddl.List {
		if s, ok := stmt.(*spansql.AlterTable); ok {
			switch s.Alteration.(type) {
			// TODO: Remove once support for Row Deletion Policy has been added in spannertest
			// See https://github.com/googleapis/google-cloud-go/issues/4782 for more details
			case spansql.AddRowDeletionPolicy:
				continue
			case spansql.ReplaceRowDeletionPolicy:
				continue
			}
		}
		stmts = append(stmts, stmt)
	}

	ddl.List = stmts
}
