package musicdb_test

import (
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spansql"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.einride.tech/spanner-aip/internal/examples/musicdb"
	"go.einride.tech/spanner-aip/spantest"
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
		actual, err := musicdb.Singers(client.Single()).Get(fx.Ctx, expected.Key())
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
			singers[1].Key(): singers[1],
			singers[3].Key(): singers[3],
			singers[5].Key(): singers[5],
		}
		actual, err := musicdb.Singers(client.Single()).BatchGet(fx.Ctx, []musicdb.SingersKey{
			singers[1].Key(),
			singers[3].Key(),
			singers[5].Key(),
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

func TestSingersParentReadTransaction(t *testing.T) {
	t.Parallel()
	if !spantest.HasDocker() {
		t.Skip("Need Docker to run Spanner emulator.")
	}
	fx := spantest.NewEmulatorDockerFixture(t)

	t.Run("insert and get", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
		expected := &musicdb.SingersParentRow{
			SingerId:  1,
			FirstName: spanner.NullString{StringVal: "Frank", Valid: true},
			LastName:  spanner.NullString{StringVal: "Sinatra", Valid: true},
			Albums: []*musicdb.AlbumsParentRow{
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
		actual, err := musicdb.SingersParent(client.Single()).Get(fx.Ctx, expected.Key())
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, actual, cmpopts.EquateEmpty())
	})

	t.Run("insert and batch get", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/music/*.up.sql")
		newSingerParent := func(i int) *musicdb.SingersParentRow {
			return &musicdb.SingersParentRow{
				SingerId:  int64(i),
				FirstName: spanner.NullString{StringVal: "Frank", Valid: true},
				LastName:  spanner.NullString{StringVal: "Sinatra", Valid: true},
				Albums: []*musicdb.AlbumsParentRow{
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
		singerParents := make([]*musicdb.SingersParentRow, 0, n)
		for i := 0; i < n; i++ {
			singerParent := newSingerParent(i)
			_, err := client.Apply(fx.Ctx, singerParent.Insert())
			assert.NilError(t, err)
			singerParents = append(singerParents, singerParent)
		}
		expected := map[musicdb.SingersKey]*musicdb.SingersParentRow{
			singerParents[1].Key(): singerParents[1],
			singerParents[3].Key(): singerParents[3],
			singerParents[5].Key(): singerParents[5],
		}
		actual, err := musicdb.SingersParent(client.Single()).BatchGet(fx.Ctx, []musicdb.SingersKey{
			singerParents[1].Key(),
			singerParents[3].Key(),
			singerParents[5].Key(),
			{SingerId: n + 1}, // not found
		})
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, actual, cmpopts.EquateEmpty())
	})
}
