package freightdb_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.einride.tech/spanner-aip/internal/examples/freightdb"
	"go.einride.tech/spanner-aip/spantest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gotest.tools/v3/assert"
)

const ddlFileGlob = "../../../testdata/migrations/freight/*.up.sql"

func TestReadTransaction(t *testing.T) {
	t.Parallel()
	fx := spantest.NewEmulatorFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	t.Run("Get", func(t *testing.T) {
		t.Parallel()
		t.Run("OK", func(t *testing.T) {
			t.Run("found", func(t *testing.T) {
				t.Parallel()
				client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
				populateDB(ctx, t, client)
				tx := client.Single()
				defer tx.Close()

				row, err := freightdb.Query(tx).GetShippersRow(ctx, freightdb.GetShippersRowQuery{
					Key: freightdb.ShippersKey{ShipperId: "allexists"},
				})
				assert.NilError(t, err)
				assert.DeepEqual(t, &freightdb.ShippersRow{ShipperId: "allexists"}, row)
			})

			t.Run("deleted", func(t *testing.T) {
				t.Parallel()
				client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
				commitTimestamp := populateDB(ctx, t, client)
				tx := client.Single()
				defer tx.Close()

				row, err := freightdb.Query(tx).GetShippersRow(ctx, freightdb.GetShippersRowQuery{
					Key: freightdb.ShippersKey{ShipperId: "deleted"},
				})
				assert.NilError(t, err)
				assert.DeepEqual(
					t,
					&freightdb.ShippersRow{
						ShipperId: "deleted",
						DeleteTime: spanner.NullTime{
							Valid: true,
							Time:  commitTimestamp,
						},
					},
					row,
				)
			})

			t.Run("interleaved", func(t *testing.T) {
				t.Parallel()
				client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
				populateDB(ctx, t, client)
				tx := client.ReadOnlyTransaction()
				defer tx.Close()

				row, err := freightdb.Query(tx).GetShippersRow(ctx, freightdb.GetShippersRowQuery{
					Key:       freightdb.ShippersKey{ShipperId: "allexists"},
					Shipments: true,
					LineItems: true,
				})
				assert.NilError(t, err)
				assert.DeepEqual(
					t,
					&freightdb.ShippersRow{
						ShipperId: "allexists",
						Shipments: []*freightdb.ShipmentsRow{
							{
								ShipperId:  "allexists",
								ShipmentId: "allexists",
								LineItems: []*freightdb.LineItemsRow{
									{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 1},
									{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 2},
								},
							},
						},
					},
					row,
				)
			})
		})

		t.Run("NotFound", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
			populateDB(ctx, t, client)
			tx := client.Single()
			defer tx.Close()

			_, err := freightdb.Query(tx).GetShippersRow(ctx, freightdb.GetShippersRowQuery{
				Key: freightdb.ShippersKey{ShipperId: "notfound"},
			})
			assert.Equal(t, codes.NotFound, status.Code(err), err)
		})
	})

	t.Run("BatchGet", func(t *testing.T) {
		t.Parallel()
		t.Run("all keys exists", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
			commitTimestamp := populateDB(ctx, t, client)
			tx := client.Single()
			defer tx.Close()

			found, err := freightdb.Query(tx).BatchGetShippersRows(ctx, freightdb.BatchGetShippersRowsQuery{
				Keys: []freightdb.ShippersKey{
					{ShipperId: "allexists"},
					{ShipperId: "deleted"},
				},
			})
			assert.NilError(t, err)
			assert.DeepEqual(
				t,
				map[freightdb.ShippersKey]*freightdb.ShippersRow{
					{ShipperId: "allexists"}: {
						ShipperId: "allexists",
					},
					{ShipperId: "deleted"}: {
						ShipperId: "deleted",
						DeleteTime: spanner.NullTime{
							Valid: true,
							Time:  commitTimestamp,
						},
					},
				},
				found,
			)
		})

		t.Run("one key missing", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
			populateDB(ctx, t, client)
			tx := client.Single()
			defer tx.Close()

			found, err := freightdb.Query(tx).BatchGetShippersRows(ctx, freightdb.BatchGetShippersRowsQuery{
				Keys: []freightdb.ShippersKey{
					{ShipperId: "allexists"},
					{ShipperId: "notfound"},
				},
			})
			assert.NilError(t, err)
			assert.DeepEqual(
				t,
				map[freightdb.ShippersKey]*freightdb.ShippersRow{
					{ShipperId: "allexists"}: {
						ShipperId: "allexists",
					},
				},
				found,
			)
		})
		t.Run("all keys exists interleaved", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
			commitTimestamp := populateDB(ctx, t, client)
			tx := client.ReadOnlyTransaction()
			defer tx.Close()

			found, err := freightdb.Query(tx).BatchGetShippersRows(ctx, freightdb.BatchGetShippersRowsQuery{
				Keys: []freightdb.ShippersKey{
					{ShipperId: "allexists"},
					{ShipperId: "deleted"},
				},
				Shipments: true,
				LineItems: true,
			})
			assert.NilError(t, err)
			assert.DeepEqual(
				t,
				map[freightdb.ShippersKey]*freightdb.ShippersRow{
					{ShipperId: "allexists"}: {
						ShipperId: "allexists",
						Shipments: []*freightdb.ShipmentsRow{
							{
								ShipperId:  "allexists",
								ShipmentId: "allexists",
								LineItems: []*freightdb.LineItemsRow{
									{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 1},
									{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 2},
								},
							},
						},
					},
					{ShipperId: "deleted"}: {
						ShipperId: "deleted",
						DeleteTime: spanner.NullTime{
							Valid: true,
							Time:  commitTimestamp,
						},
						Shipments: []*freightdb.ShipmentsRow{
							{
								ShipperId:  "deleted",
								ShipmentId: "deleted",
								LineItems: []*freightdb.LineItemsRow{
									{ShipperId: "deleted", ShipmentId: "deleted", LineNumber: 1},
									{ShipperId: "deleted", ShipmentId: "deleted", LineNumber: 2},
								},
							},
						},
					},
				},
				found,
			)
		})

		t.Run("one key missing interleaved", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
			populateDB(ctx, t, client)
			tx := client.ReadOnlyTransaction()
			defer tx.Close()

			found, err := freightdb.Query(tx).BatchGetShippersRows(ctx, freightdb.BatchGetShippersRowsQuery{
				Keys: []freightdb.ShippersKey{
					{ShipperId: "allexists"},
					{ShipperId: "notfound"},
				},
				Shipments: true,
				LineItems: true,
			})
			assert.NilError(t, err)
			assert.DeepEqual(
				t,
				map[freightdb.ShippersKey]*freightdb.ShippersRow{
					{ShipperId: "allexists"}: {
						ShipperId: "allexists",
						Shipments: []*freightdb.ShipmentsRow{
							{
								ShipperId:  "allexists",
								ShipmentId: "allexists",
								LineItems: []*freightdb.LineItemsRow{
									{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 1},
									{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 2},
								},
							},
						},
					},
				},
				found,
			)
		})
	})

	t.Run("List", func(t *testing.T) {
		t.Parallel()
		t.Run("hide deleted by default", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
			populateDB(ctx, t, client)
			tx := client.Single()
			defer tx.Close()

			var got []*freightdb.ShippersRow
			assert.NilError(t, freightdb.Query(tx).ListShippersRows(ctx, freightdb.ListShippersRowsQuery{
				Limit: 10,
			}).Do(func(row *freightdb.ShippersRow) error {
				got = append(got, row)
				return nil
			}))
			assert.DeepEqual(
				t,
				[]*freightdb.ShippersRow{
					{ShipperId: "allexists"},
					{ShipperId: "interleavedeleted"},
				},
				got,
			)
		})

		t.Run("show deleted", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
			commitTimestamp := populateDB(ctx, t, client)
			tx := client.Single()
			defer tx.Close()

			var got []*freightdb.ShippersRow
			assert.NilError(t, freightdb.Query(tx).ListShippersRows(ctx, freightdb.ListShippersRowsQuery{
				Limit:       10,
				ShowDeleted: true,
			}).Do(func(row *freightdb.ShippersRow) error {
				got = append(got, row)
				return nil
			}))
			assert.DeepEqual(
				t,
				[]*freightdb.ShippersRow{
					{ShipperId: "allexists"},
					{
						ShipperId: "deleted",
						DeleteTime: spanner.NullTime{
							Valid: true,
							Time:  commitTimestamp,
						},
					},
					{ShipperId: "interleavedeleted"},
				},
				got,
			)
		})

		t.Run("interleaved", func(t *testing.T) {
			t.Parallel()
			client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
			populateDB(ctx, t, client)
			tx := client.ReadOnlyTransaction()
			defer tx.Close()

			var got []*freightdb.ShippersRow
			assert.NilError(t, freightdb.Query(tx).ListShippersRows(ctx, freightdb.ListShippersRowsQuery{
				Limit:     10,
				Shipments: true,
				LineItems: true,
			}).Do(func(row *freightdb.ShippersRow) error {
				got = append(got, row)
				return nil
			}))
			assert.DeepEqual(
				t,
				[]*freightdb.ShippersRow{
					{
						ShipperId: "allexists",
						Shipments: []*freightdb.ShipmentsRow{
							{
								ShipperId:  "allexists",
								ShipmentId: "allexists",
								LineItems: []*freightdb.LineItemsRow{
									{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 1},
									{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 2},
								},
							},
						},
					},
					{
						ShipperId: "interleavedeleted",
						Shipments: []*freightdb.ShipmentsRow{
							{
								ShipperId:  "interleavedeleted",
								ShipmentId: "interleavedeleted",
								LineItems: []*freightdb.LineItemsRow{
									{ShipperId: "interleavedeleted", ShipmentId: "interleavedeleted", LineNumber: 1},
									{ShipperId: "interleavedeleted", ShipmentId: "interleavedeleted", LineNumber: 2},
								},
							},
						},
					},
				},
				got,
				cmpopts.IgnoreFields(freightdb.ShipmentsRow{}, "DeleteTime"),
			)
		})
	})

	t.Run("hide deleted by default", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
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

	t.Run("show deleted", func(t *testing.T) {
		t.Parallel()
		client := fx.NewDatabaseFromDDLFiles(t, ddlFileGlob)
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

func populateDB(ctx context.Context, t *testing.T, client *spanner.Client) time.Time {
	t.Helper()

	shippers := []*freightdb.ShippersRow{
		{
			ShipperId: "allexists",
			Shipments: []*freightdb.ShipmentsRow{
				{
					ShipperId:  "allexists",
					ShipmentId: "allexists",
					LineItems: []*freightdb.LineItemsRow{
						{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 1},
						{ShipperId: "allexists", ShipmentId: "allexists", LineNumber: 2},
					},
				},
			},
		},
		{
			ShipperId: "deleted",
			DeleteTime: spanner.NullTime{
				Valid: true,
				Time:  spanner.CommitTimestamp,
			},
			Shipments: []*freightdb.ShipmentsRow{
				{
					ShipperId:  "deleted",
					ShipmentId: "deleted",
					LineItems: []*freightdb.LineItemsRow{
						{ShipperId: "deleted", ShipmentId: "deleted", LineNumber: 1},
						{ShipperId: "deleted", ShipmentId: "deleted", LineNumber: 2},
					},
				},
			},
		},
		{
			ShipperId: "interleavedeleted",
			Shipments: []*freightdb.ShipmentsRow{
				{
					ShipperId:  "interleavedeleted",
					ShipmentId: "interleavedeleted",
					DeleteTime: spanner.NullTime{
						Valid: true,
						Time:  spanner.CommitTimestamp,
					},
					LineItems: []*freightdb.LineItemsRow{
						{ShipperId: "interleavedeleted", ShipmentId: "interleavedeleted", LineNumber: 1},
						{ShipperId: "interleavedeleted", ShipmentId: "interleavedeleted", LineNumber: 2},
					},
				},
			},
		},
	}
	mutations := make([]*spanner.Mutation, 0, 20)
	for _, shipper := range shippers {
		mutations = append(mutations, spanner.Insert(shipper.Mutate()))
		for _, shipment := range shipper.Shipments {
			mutations = append(mutations, spanner.Insert(shipment.Mutate()))
			for _, lineItem := range shipment.LineItems {
				mutations = append(mutations, spanner.Insert(lineItem.Mutate()))
			}
		}
	}
	commitTimestamp, err := client.Apply(ctx, mutations)
	assert.NilError(t, err)

	return commitTimestamp
}
