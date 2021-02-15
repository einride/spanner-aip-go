package freightdb_test

import (
	"context"
	"strconv"
	"testing"

	"cloud.google.com/go/spanner"
	"go.einride.tech/spanner-aip/internal/examples/freightdb"
	"go.einride.tech/spanner-aip/spantest"
	"gotest.tools/v3/assert"
)

func TestReadTransaction(t *testing.T) {
	t.Parallel()
	fx := spantest.NewEmulatorFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

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
		_, err := client.Apply(ctx, mutations)
		assert.NilError(t, err)
		gotIDs := make([]string, 0, count)
		tx := client.Single()
		defer tx.Close()
		assert.NilError(t, freightdb.Query(tx).ListShippersRows(ctx, freightdb.ListShippersRowsQuery{
			Limit: count,
		}).Do(func(row *freightdb.ShippersRow) error {
			gotIDs = append(gotIDs, row.ShipperId)
			return nil
		}))
		assert.DeepEqual(t, expectedIDs, gotIDs)
	})

	t.Run("get deleted interleaved", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, "../../../testdata/migrations/freight/*.up.sql")
		const count = 10
		mutations := make([]*spanner.Mutation, 0, count)
		shipper := &freightdb.ShippersRow{
			ShipperId: "shipper",
			DeleteTime: spanner.NullTime{
				Time:  spanner.CommitTimestamp,
				Valid: true,
			},
		}
		mutations = append(mutations, spanner.Insert(shipper.Mutate()))
		expectedShipmentIDs := make([]string, 0, count)
		for i := 0; i < count; i++ {
			shipment := &freightdb.ShipmentsRow{ShipperId: shipper.ShipperId, ShipmentId: strconv.Itoa(i)}
			if i%2 == 1 {
				shipment.DeleteTime = spanner.NullTime{
					Time:  spanner.CommitTimestamp,
					Valid: true,
				}
			}
			expectedShipmentIDs = append(expectedShipmentIDs, shipment.ShipmentId)
			mutations = append(mutations, spanner.Insert(shipment.Mutate()))
		}
		_, err := client.Apply(ctx, mutations)
		assert.NilError(t, err)
		tx := client.Single()
		defer tx.Close()
		gotShipper, err := freightdb.Query(tx).GetShippersRow(ctx, freightdb.GetShippersRowQuery{
			Key:       freightdb.ShippersKey{ShipperId: shipper.ShipperId},
			Shipments: true,
		})
		assert.NilError(t, err)
		gotShipmentIDs := make([]string, 0, len(gotShipper.Shipments))
		for _, gotShipment := range gotShipper.Shipments {
			gotShipmentIDs = append(gotShipmentIDs, gotShipment.ShipmentId)
		}
		assert.DeepEqual(t, expectedShipmentIDs, gotShipmentIDs)
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
		_, err := client.Apply(ctx, mutations)
		assert.NilError(t, err)
		gotIDs := make([]string, 0, count)
		tx := client.Single()
		defer tx.Close()
		assert.NilError(t, freightdb.Query(tx).ListShippersRows(ctx, freightdb.ListShippersRowsQuery{
			Limit:       count,
			ShowDeleted: true,
		}).Do(func(row *freightdb.ShippersRow) error {
			gotIDs = append(gotIDs, row.ShipperId)
			return nil
		}))
		assert.DeepEqual(t, expectedIDs, gotIDs)
	})
}
