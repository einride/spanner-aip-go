package musicdb_test

import (
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spansql"
	"go.einride.tech/aip-spanner/internal/examples/musicdb"
	"go.einride.tech/aip-spanner/spantest"
	"gotest.tools/v3/assert"
)

func TestAlbumsReadTransaction(t *testing.T) {
	t.Parallel()
	if !spantest.HasDocker() {
		t.Skip("Need Docker to run Spanner emulator.")
	}
	fx := spantest.NewEmulatorDockerFixture(t)

	t.Run("insert and get", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
		expected := &musicdb.SingersRow{
			SingerId:  1,
			FirstName: spanner.NullString{StringVal: "Frank", Valid: true},
			LastName:  spanner.NullString{StringVal: "Sinatra", Valid: true},
		}
		_, err := client.Apply(fx.Ctx, []*spanner.Mutation{expected.Insert()})
		assert.NilError(t, err)
		actual, err := musicdb.Singers(client.Single()).Get(fx.Ctx, expected.PrimaryKey())
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
			_, err := client.Apply(fx.Ctx, []*spanner.Mutation{singer.Insert()})
			assert.NilError(t, err)
			singers = append(singers, singer)
		}
		expected := map[musicdb.SingersKey]*musicdb.SingersRow{
			singers[1].PrimaryKey(): singers[1],
			singers[3].PrimaryKey(): singers[3],
			singers[5].PrimaryKey(): singers[5],
		}
		actual, err := musicdb.Singers(client.Single()).BatchGet(fx.Ctx, []musicdb.SingersKey{
			singers[1].PrimaryKey(),
			singers[3].PrimaryKey(),
			singers[5].PrimaryKey(),
			{SingerId: n + 1}, // not found
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
			_, err := client.Apply(fx.Ctx, []*spanner.Mutation{singer.Insert()})
			assert.NilError(t, err)
			expected = append(expected, singer)
		}
		var actual []*musicdb.SingersRow
		const pageSize = 10
		for i := int64(0); i < n/pageSize; i++ {
			assert.NilError(t, musicdb.Singers(client.Single()).List(fx.Ctx, musicdb.ListQuery{
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
}

func TestSingersAndAlbumsReadTransaction(t *testing.T) {
	t.Parallel()
	if !spantest.HasDocker() {
		t.Skip("Need Docker to run Spanner emulator.")
	}
	fx := spantest.NewEmulatorDockerFixture(t)

	t.Run("insert and get", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
		expected := &musicdb.SingersAndAlbumsRow{
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
		_, err := client.Apply(fx.Ctx, expected.Insert())
		assert.NilError(t, err)
		actual, err := musicdb.SingersAndAlbums(client.Single()).Get(fx.Ctx, expected.SingersKey())
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, actual)
	})

	t.Run("insert and batch get", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
		newSingerAndAlbums := func(i int) *musicdb.SingersAndAlbumsRow {
			return &musicdb.SingersAndAlbumsRow{
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
		singersAndAlbums := make([]*musicdb.SingersAndAlbumsRow, 0, n)
		for i := 0; i < n; i++ {
			singerAndAlbums := newSingerAndAlbums(i)
			_, err := client.Apply(fx.Ctx, singerAndAlbums.Insert())
			assert.NilError(t, err)
			singersAndAlbums = append(singersAndAlbums, singerAndAlbums)
		}
		expected := map[musicdb.SingersKey]*musicdb.SingersAndAlbumsRow{
			singersAndAlbums[1].SingersKey(): singersAndAlbums[1],
			singersAndAlbums[3].SingersKey(): singersAndAlbums[3],
			singersAndAlbums[5].SingersKey(): singersAndAlbums[5],
		}
		actual, err := musicdb.SingersAndAlbums(client.Single()).BatchGet(fx.Ctx, []musicdb.SingersKey{
			singersAndAlbums[1].SingersKey(),
			singersAndAlbums[3].SingersKey(),
			singersAndAlbums[5].SingersKey(),
			{SingerId: n + 1}, // not found
		})
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, actual)
	})
}
