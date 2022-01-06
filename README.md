Spanner AIP Go
==============

[![PkgGoDev](https://pkg.go.dev/badge/go.einride.tech/spanner-aip)](https://pkg.go.dev/go.einride.tech/spanner-aip) [![GoReportCard](https://goreportcard.com/badge/go.einride.tech/spanner-aip)](https://goreportcard.com/report/go.einride.tech/spanner-aip) [![Codecov](https://codecov.io/gh/einride/spanner-aip-go/branch/master/graph/badge.svg)](https://codecov.io/gh/einride/spanner-aip-go)

Add-on to the [AIP Go SDK](https://github.com/einride/aip-go) for implementing [Cloud Spanner](https://cloud.google.com/spanner) persistance for [resource-oriented APIs](https://aip.dev).

**Experimental**: This library is under active development and breaking changes to config files, APIs and generated code are expected between releases.

Documentation
-------------

See [https://aip.dev](https://aip.dev) for the full AIP documentation and the [Cloud Spanner documentation](https://cloud.google.com/spanner/docs).

Usage
-----

### Installing

```bash
$ go get -u go.einride.tech/spanner-aip
```

### Code generation config

Use a YAML config file to specify the schema to generate code from:

```yaml
databases:
  - name: music
    schema:
      - "testdata/migrations/music/*.up.sql"
    package:
      name: musicdb
      path: ./internal/examples/musicdb
```

### Code generation

```bash
$ go run go.einride.tech/spanner-aip generate
```

### Reading data

#### Get

```go
package main

import (
	"context"

	"cloud.google.com/go/spanner"
	"go.einride.tech/spanner-aip/internal/examples/musicdb"
)

func main() {
	ctx := context.Background()
	client, err := spanner.NewClient(
		ctx, "projects/<PROJECT>/instances/<INSTANCE>/databases/<DATABASE>",
	)
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	singer, err := musicdb.Singers(client.Single()).Get(ctx, musicdb.SingersKey{
		SingerId: 42,
	})
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	_ = singer // TODO: Use singer.
}
```

#### List

```go
package main

import (
	"context"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/spanner-aip/internal/examples/musicdb"
)

func main() {
	ctx := context.Background()
	client, err := spanner.NewClient(
		ctx, "projects/<PROJECT>/instances/<INSTANCE>/databases/<DATABASE>",
	)
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	// SELECT * FROM Singers
	// WHERE LastName = "Sinatra"
	// ORDER BY FirstName DESC
	// LIMIT 5
	// OFFSET 10
	if err := musicdb.Singers(client.Single()).List(ctx, musicdb.ListQuery{
		Where: spansql.ComparisonOp{
			Op:  spansql.Eq,
			LHS: musicdb.Descriptor().Singers().LastName().ColumnID(),
			RHS: spansql.StringLiteral("Sinatra"),
		},
		Order: []spansql.Order{
			{Expr: musicdb.Descriptor().Singers().FirstName().ColumnID(), Desc: true},
		},
		Limit:  5,
		Offset: 10,
	}).Do(func(singer *musicdb.SingersRow) error {
		_ = singer // TODO: Use singer.
		return nil
	}); err != nil {
		panic(err) // TODO: Handle error.
	}
}
```
