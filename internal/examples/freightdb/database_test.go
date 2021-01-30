package freightdb_test

import (
	"strconv"
	"testing"

	"cloud.google.com/go/spanner"
	"go.einride.tech/spanner-aip/internal/examples/freightdb"
	"go.einride.tech/spanner-aip/spantest"
	"gotest.tools/v3/assert"
)

func TestReadTransaction(t *testing.T) {
	t.Parallel()
	fx := spantest.NewEmulatorDockerFixture(t)

	t.Run("hide deleted by default", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/freight/*.up.sql")
		const count = 10
		mutations := make([]*spanner.Mutation, 0, count)
		expectedIDs := make([]string, 0, count)
		for i := 0; i < count; i++ {
			shipper := &freightdb.ShippersRow{ShipperId: strconv.Itoa(i)}
			if i%2 == 1 {
				shipper.DeleteTime = spanner.NullTime{
					Time:  spanner.CommitTimestamp,
					Valid: true,
				}
			} else {
				expectedIDs = append(expectedIDs, shipper.ShipperId)
			}
			mutations = append(mutations, spanner.Insert(shipper.Mutate()))
		}
		_, err := client.Apply(fx.Ctx, mutations)
		assert.NilError(t, err)
		gotIDs := make([]string, 0, count)
		tx := client.Single()
		defer tx.Close()
		assert.NilError(t, freightdb.Query(tx).ListShippersRows(fx.Ctx, freightdb.ListShippersRowsQuery{
			Limit: count,
		}).Do(func(row *freightdb.ShippersRow) error {
			gotIDs = append(gotIDs, row.ShipperId)
			return nil
		}))
		assert.DeepEqual(t, expectedIDs, gotIDs)
	})

	t.Run("show deleted", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/freight/*.up.sql")
		const count = 10
		mutations := make([]*spanner.Mutation, 0, count)
		expectedIDs := make([]string, 0, count)
		for i := 0; i < count; i++ {
			shipper := &freightdb.ShippersRow{ShipperId: strconv.Itoa(i)}
			if i%2 == 1 {
				shipper.DeleteTime = spanner.NullTime{
					Time:  spanner.CommitTimestamp,
					Valid: true,
				}
			}
			expectedIDs = append(expectedIDs, shipper.ShipperId)
			mutations = append(mutations, spanner.Insert(shipper.Mutate()))
		}
		_, err := client.Apply(fx.Ctx, mutations)
		assert.NilError(t, err)
		gotIDs := make([]string, 0, count)
		tx := client.Single()
		defer tx.Close()
		assert.NilError(t, freightdb.Query(tx).ListShippersRows(fx.Ctx, freightdb.ListShippersRowsQuery{
			Limit:       count,
			ShowDeleted: true,
		}).Do(func(row *freightdb.ShippersRow) error {
			gotIDs = append(gotIDs, row.ShipperId)
			return nil
		}))
		assert.DeepEqual(t, expectedIDs, gotIDs)
	})
}
