package musicdb_test

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/spanner-aip/internal/examples/musicdb"
	"go.einride.tech/spanner-aip/spantest"
	"gotest.tools/v3/assert"
)

func TestReadTransaction(t *testing.T) {
	t.Parallel()
	fx := spantest.NewEmulatorFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	t.Run("insert and get", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
		expected := &musicdb.SingersRow{
			SingerId:  1,
			FirstName: spanner.NullString{StringVal: "Frank", Valid: true},
			LastName:  spanner.NullString{StringVal: "Sinatra", Valid: true},
		}
		_, err := client.Apply(ctx, []*spanner.Mutation{spanner.Insert(expected.Mutate())})
		assert.NilError(t, err)
		tx := client.Single()
		defer tx.Close()
		actual, err := musicdb.Query(tx).GetSingersRow(ctx, musicdb.GetSingersRowQuery{
			Key: expected.Key(),
		})
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, actual)
	})

	t.Run("insert and batch get", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
		newSinger := func(i int) *musicdb.SingersRow {
			return &musicdb.SingersRow{
				SingerId:  int64(i),
				FirstName: spanner.NullString{StringVal: "Frank", Valid: true},
				LastName:  spanner.NullString{StringVal: "Sinatra", Valid: true},
			}
		}
		const n = 10
		singers := make([]*musicdb.SingersRow, 0, n)
		for i := 0; i < n; i++ {
			singer := newSinger(i)
			_, err := client.Apply(ctx, []*spanner.Mutation{spanner.Insert(singer.Mutate())})
			assert.NilError(t, err)
			singers = append(singers, singer)
		}
		expected := map[musicdb.SingersKey]*musicdb.SingersRow{
			singers[1].Key(): singers[1],
			singers[3].Key(): singers[3],
			singers[5].Key(): singers[5],
		}
		tx := client.Single()
		defer tx.Close()
		actual, err := musicdb.Query(tx).BatchGetSingersRows(ctx, musicdb.BatchGetSingersRowsQuery{
			Keys: []musicdb.SingersKey{
				singers[1].Key(),
				singers[3].Key(),
				singers[5].Key(),
				{SingerId: n + 1}, // not found
			},
		})
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, actual)
	})

	t.Run("insert many and list pages", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
		newSinger := func(i int) *musicdb.SingersRow {
			return &musicdb.SingersRow{
				SingerId:  int64(i),
				FirstName: spanner.NullString{StringVal: "Frank", Valid: true},
				LastName:  spanner.NullString{StringVal: "Sinatra", Valid: true},
			}
		}
		const n = 1000
		expected := make([]*musicdb.SingersRow, 0, n)
		for i := 0; i < n; i++ {
			singer := newSinger(i)
			_, err := client.Apply(ctx, []*spanner.Mutation{spanner.Insert(singer.Mutate())})
			assert.NilError(t, err)
			expected = append(expected, singer)
		}
		var actual []*musicdb.SingersRow
		const pageSize = 10
		tx := client.ReadOnlyTransaction()
		defer tx.Close()
		for i := int64(0); i < n/pageSize; i++ {
			assert.NilError(t, musicdb.Query(tx).ListSingersRows(ctx, musicdb.ListSingersRowsQuery{
				Order: []spansql.Order{
					{Expr: musicdb.Descriptor().Singers().SingerId().ColumnID()},
				},
				Limit:  10,
				Offset: pageSize * i,
			}).Do(func(row *musicdb.SingersRow) error {
				actual = append(actual, row)
				return nil
			}))
		}
		assert.DeepEqual(t, expected, actual)
	})

	t.Run("interleaved", func(t *testing.T) {
		t.Run("insert and get", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
			expected := &musicdb.SingersRow{
				SingerId:  1,
				FirstName: spanner.NullString{StringVal: "Frank", Valid: true},
				LastName:  spanner.NullString{StringVal: "Sinatra", Valid: true},
				Albums: []*musicdb.AlbumsRow{
					{
						SingerId: 1,
						AlbumId:  1,
						AlbumTitle: spanner.NullString{
							StringVal: "Test1",
							Valid:     true,
						},
					},
					{
						SingerId: 1,
						AlbumId:  2,
						AlbumTitle: spanner.NullString{
							StringVal: "Test2",
							Valid:     true,
						},
					},
					{
						SingerId: 1,
						AlbumId:  3,
						AlbumTitle: spanner.NullString{
							StringVal: "Test3",
							Valid:     true,
						},
					},
				},
			}
			mutations := []*spanner.Mutation{spanner.Insert(expected.Mutate())}
			for _, album := range expected.Albums {
				mutations = append(mutations, spanner.Insert(album.Mutate()))
			}
			_, err := client.Apply(ctx, mutations)
			assert.NilError(t, err)
			tx := client.ReadOnlyTransaction()
			actual, err := musicdb.Query(tx).GetSingersRow(ctx, musicdb.GetSingersRowQuery{
				Key:    expected.Key(),
				Albums: true,
			})
			assert.NilError(t, err)
			assert.DeepEqual(t, expected, actual)
		})

		t.Run("insert and batch get", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
			newSingerAndAlbums := func(i int) *musicdb.SingersRow {
				return &musicdb.SingersRow{
					SingerId:  int64(i),
					FirstName: spanner.NullString{StringVal: "Frank", Valid: true},
					LastName:  spanner.NullString{StringVal: "Sinatra", Valid: true},
					Albums: []*musicdb.AlbumsRow{
						{
							SingerId: int64(i),
							AlbumId:  1,
							AlbumTitle: spanner.NullString{
								StringVal: "Test1",
								Valid:     true,
							},
						},
						{
							SingerId: int64(i),
							AlbumId:  2,
							AlbumTitle: spanner.NullString{
								StringVal: "Test2",
								Valid:     true,
							},
						},
						{
							SingerId: int64(i),
							AlbumId:  3,
							AlbumTitle: spanner.NullString{
								StringVal: "Test3",
								Valid:     true,
							},
						},
					},
				}
			}
			const n = 10
			singersAndAlbums := make([]*musicdb.SingersRow, 0, n)
			for i := 0; i < n; i++ {
				singerAndAlbums := newSingerAndAlbums(i)
				mutations := []*spanner.Mutation{spanner.Insert(singerAndAlbums.Mutate())}
				for _, album := range singerAndAlbums.Albums {
					mutations = append(mutations, spanner.Insert(album.Mutate()))
				}
				_, err := client.Apply(ctx, mutations)
				assert.NilError(t, err)
				singersAndAlbums = append(singersAndAlbums, singerAndAlbums)
			}
			expected := map[musicdb.SingersKey]*musicdb.SingersRow{
				singersAndAlbums[1].Key(): singersAndAlbums[1],
				singersAndAlbums[3].Key(): singersAndAlbums[3],
				singersAndAlbums[5].Key(): singersAndAlbums[5],
			}
			tx := client.Single()
			actual, err := musicdb.Query(tx).BatchGetSingersRows(ctx, musicdb.BatchGetSingersRowsQuery{
				Keys: []musicdb.SingersKey{
					singersAndAlbums[1].Key(),
					singersAndAlbums[3].Key(),
					singersAndAlbums[5].Key(),
					{SingerId: n + 1}, // not found
				},
				Albums: true,
			})
			assert.NilError(t, err)
			assert.DeepEqual(t, expected, actual)
		})
	})
}
