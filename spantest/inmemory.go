package spantest

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spannertest"
	"cloud.google.com/go/spanner/spansql"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"gotest.tools/v3/assert"
)

// InMemoryFixture is a test fixture running the Spanner emulator.
type InMemoryFixture struct {
	ctx context.Context
}

// NewInMemoryFixture creates a test fixture for the in-memory Spanner emulator.
func NewInMemoryFixture(t *testing.T) Fixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if deadline, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
	}
	return &InMemoryFixture{ctx: ctx}
}

// NewDatabaseFromDDLFiles implements Fixture.
func (fx *InMemoryFixture) NewDatabaseFromDDLFiles(t *testing.T, globs ...string) *spanner.Client {
	t.Helper()
	var files []string
	for _, glob := range globs {
		globFiles, err := filepath.Glob(glob)
		assert.NilError(t, err)
		files = append(files, globFiles...)
	}
	var statements []string
	for _, file := range files {
		content, err := ioutil.ReadFile(file)
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
func (fx *InMemoryFixture) NewDatabaseFromStatements(t *testing.T, statements []string) *spanner.Client {
	t.Helper()
	const (
		projectID  = "spanner-aip-go"
		instanceID = "in-memory"
	)
	databaseID := "db" + strconv.Itoa(rand.Int()) // nolint: gosec
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	server, err := spannertest.NewServer("localhost:0")
	assert.NilError(t, err)
	t.Cleanup(server.Close)
	conn, err := grpc.Dial(server.Addr, grpc.WithInsecure())
	assert.NilError(t, err)
	client, err := spanner.NewClient(fx.ctx, databaseName, option.WithGRPCConn(conn))
	assert.NilError(t, err)
	t.Cleanup(client.Close)
	for i, statement := range statements {
		ddl, err := spansql.ParseDDL(fmt.Sprintf("statement%d", i), statement)
		assert.NilError(t, err)
		assert.NilError(t, server.UpdateDDL(ddl))
	}
	return client
}
