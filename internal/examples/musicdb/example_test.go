package musicdb_test

import (
	"context"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip-spanner/internal/examples/musicdb"
)

func ExampleAlbumsReadTransaction_Get() {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, "projects/<PROJECT>/instances/<INSTANCE>/databases/<DATABASE>")
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	singer, err := musicdb.Singers(client.Single()).Get(ctx, musicdb.SingersPrimaryKey{
		SingerId: 42,
	})
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	_ = singer // TODO: Use singer.
}

func ExampleAlbumsReadTransaction_List() {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, "projects/<PROJECT>/instances/<INSTANCE>/databases/<DATABASE>")
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	// SELECT * FROM Singers WHERE LastName = "Sinatra" ORDER BY FirstName DESC LIMIT 5 OFFSET 10
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

func Example_readOnlyTransaction_MultipleTables() {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, "projects/<PROJECT>/instances/<INSTANCE>/databases/<DATABASE>")
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	tx := client.ReadOnlyTransaction()
	defer tx.Close()
	singer, err := musicdb.Singers(tx).Get(ctx, musicdb.SingersPrimaryKey{
		SingerId: 42,
	})
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	album, err := musicdb.Albums(tx).Get(ctx, musicdb.AlbumsPrimaryKey{
		SingerId: 42,
		AlbumId:  24,
	})
	if err != nil {
		panic(err) // TODO: Handle error.
	}
	_ = singer // TODO: Use singer.
	_ = album  // TODO: Use album.
}
