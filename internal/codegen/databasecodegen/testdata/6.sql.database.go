// Code generated by TestDatabaseCodeGenerator_GenerateCode/database/testdata/6.sql. DO NOT EDIT.
//go:build testdata.6.sql.database
// +build testdata.6.sql.database

package testdata

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spansql"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

type ShippersRow struct {
	ShipperId  string           `spanner:"shipper_id"`
	CreateTime time.Time        `spanner:"create_time"`
	UpdateTime time.Time        `spanner:"update_time"`
	DeleteTime spanner.NullTime `spanner:"delete_time"`
	Shipments  []*ShipmentsRow  `spanner:"shipments"`
}

func (*ShippersRow) ColumnNames() []string {
	return []string{
		"shipper_id",
		"create_time",
		"update_time",
		"delete_time",
	}
}

func (*ShippersRow) ColumnIDs() []spansql.ID {
	return []spansql.ID{
		"shipper_id",
		"create_time",
		"update_time",
		"delete_time",
	}
}

func (*ShippersRow) ColumnExprs() []spansql.Expr {
	return []spansql.Expr{
		spansql.ID("shipper_id"),
		spansql.ID("create_time"),
		spansql.ID("update_time"),
		spansql.ID("delete_time"),
	}
}

func (r *ShippersRow) Validate() error {
	if len(r.ShipperId) > 63 {
		return fmt.Errorf("column shipper_id length > 63")
	}
	return nil
}

func (r *ShippersRow) UnmarshalSpannerRow(row *spanner.Row) error {
	for i := 0; i < row.Size(); i++ {
		switch row.ColumnName(i) {
		case "shipper_id":
			if err := row.Column(i, &r.ShipperId); err != nil {
				return fmt.Errorf("unmarshal shippers row: shipper_id column: %w", err)
			}
		case "create_time":
			if err := row.Column(i, &r.CreateTime); err != nil {
				return fmt.Errorf("unmarshal shippers row: create_time column: %w", err)
			}
		case "update_time":
			if err := row.Column(i, &r.UpdateTime); err != nil {
				return fmt.Errorf("unmarshal shippers row: update_time column: %w", err)
			}
		case "delete_time":
			if err := row.Column(i, &r.DeleteTime); err != nil {
				return fmt.Errorf("unmarshal shippers row: delete_time column: %w", err)
			}
		case "shipments":
			if err := row.Column(i, &r.Shipments); err != nil {
				return fmt.Errorf("unmarshal shippers interleaved row: shipments column: %w", err)
			}
		default:
			return fmt.Errorf("unmarshal shippers row: unhandled column: %s", row.ColumnName(i))
		}
	}
	return nil
}

func (r *ShippersRow) Mutate() (string, []string, []interface{}) {
	return "shippers", r.ColumnNames(), []interface{}{
		r.ShipperId,
		r.CreateTime,
		r.UpdateTime,
		r.DeleteTime,
	}
}

func (r *ShippersRow) MutateColumns(columns []string) (string, []string, []interface{}) {
	if len(columns) == 0 {
		columns = r.ColumnNames()
	}
	values := make([]interface{}, 0, len(columns))
	for _, column := range columns {
		switch column {
		case "shipper_id":
			values = append(values, r.ShipperId)
		case "create_time":
			values = append(values, r.CreateTime)
		case "update_time":
			values = append(values, r.UpdateTime)
		case "delete_time":
			values = append(values, r.DeleteTime)
		default:
			panic(fmt.Errorf("table shippers does not have column %s", column))
		}
	}
	return "shippers", columns, values
}

func (r *ShippersRow) MutatePresentColumns() (string, []string, []interface{}) {
	columns := make([]string, 0, len(r.ColumnNames()))
	columns = append(
		columns,
		"shipper_id",
		"create_time",
		"update_time",
	)
	if !r.DeleteTime.IsNull() {
		columns = append(columns, "delete_time")
	}
	return r.MutateColumns(columns)
}

func (r *ShippersRow) Key() ShippersKey {
	return ShippersKey{
		ShipperId: r.ShipperId,
	}
}

type ShipmentsRow struct {
	ShipperId  string           `spanner:"shipper_id"`
	ShipmentId string           `spanner:"shipment_id"`
	CreateTime time.Time        `spanner:"create_time"`
	UpdateTime time.Time        `spanner:"update_time"`
	DeleteTime spanner.NullTime `spanner:"delete_time"`
}

func (*ShipmentsRow) ColumnNames() []string {
	return []string{
		"shipper_id",
		"shipment_id",
		"create_time",
		"update_time",
		"delete_time",
	}
}

func (*ShipmentsRow) ColumnIDs() []spansql.ID {
	return []spansql.ID{
		"shipper_id",
		"shipment_id",
		"create_time",
		"update_time",
		"delete_time",
	}
}

func (*ShipmentsRow) ColumnExprs() []spansql.Expr {
	return []spansql.Expr{
		spansql.ID("shipper_id"),
		spansql.ID("shipment_id"),
		spansql.ID("create_time"),
		spansql.ID("update_time"),
		spansql.ID("delete_time"),
	}
}

func (r *ShipmentsRow) Validate() error {
	if len(r.ShipperId) > 63 {
		return fmt.Errorf("column shipper_id length > 63")
	}
	if len(r.ShipmentId) > 63 {
		return fmt.Errorf("column shipment_id length > 63")
	}
	return nil
}

func (r *ShipmentsRow) UnmarshalSpannerRow(row *spanner.Row) error {
	for i := 0; i < row.Size(); i++ {
		switch row.ColumnName(i) {
		case "shipper_id":
			if err := row.Column(i, &r.ShipperId); err != nil {
				return fmt.Errorf("unmarshal shipments row: shipper_id column: %w", err)
			}
		case "shipment_id":
			if err := row.Column(i, &r.ShipmentId); err != nil {
				return fmt.Errorf("unmarshal shipments row: shipment_id column: %w", err)
			}
		case "create_time":
			if err := row.Column(i, &r.CreateTime); err != nil {
				return fmt.Errorf("unmarshal shipments row: create_time column: %w", err)
			}
		case "update_time":
			if err := row.Column(i, &r.UpdateTime); err != nil {
				return fmt.Errorf("unmarshal shipments row: update_time column: %w", err)
			}
		case "delete_time":
			if err := row.Column(i, &r.DeleteTime); err != nil {
				return fmt.Errorf("unmarshal shipments row: delete_time column: %w", err)
			}
		default:
			return fmt.Errorf("unmarshal shipments row: unhandled column: %s", row.ColumnName(i))
		}
	}
	return nil
}

func (r *ShipmentsRow) Mutate() (string, []string, []interface{}) {
	return "shipments", r.ColumnNames(), []interface{}{
		r.ShipperId,
		r.ShipmentId,
		r.CreateTime,
		r.UpdateTime,
		r.DeleteTime,
	}
}

func (r *ShipmentsRow) MutateColumns(columns []string) (string, []string, []interface{}) {
	if len(columns) == 0 {
		columns = r.ColumnNames()
	}
	values := make([]interface{}, 0, len(columns))
	for _, column := range columns {
		switch column {
		case "shipper_id":
			values = append(values, r.ShipperId)
		case "shipment_id":
			values = append(values, r.ShipmentId)
		case "create_time":
			values = append(values, r.CreateTime)
		case "update_time":
			values = append(values, r.UpdateTime)
		case "delete_time":
			values = append(values, r.DeleteTime)
		default:
			panic(fmt.Errorf("table shipments does not have column %s", column))
		}
	}
	return "shipments", columns, values
}

func (r *ShipmentsRow) MutatePresentColumns() (string, []string, []interface{}) {
	columns := make([]string, 0, len(r.ColumnNames()))
	columns = append(
		columns,
		"shipper_id",
		"shipment_id",
		"create_time",
		"update_time",
	)
	if !r.DeleteTime.IsNull() {
		columns = append(columns, "delete_time")
	}
	return r.MutateColumns(columns)
}

func (r *ShipmentsRow) Key() ShipmentsKey {
	return ShipmentsKey{
		ShipperId:  r.ShipperId,
		ShipmentId: r.ShipmentId,
	}
}

type ShippersKey struct {
	ShipperId string
}

func (k ShippersKey) SpannerKey() spanner.Key {
	return spanner.Key{
		k.ShipperId,
	}
}

func (k ShippersKey) SpannerKeySet() spanner.KeySet {
	return k.SpannerKey()
}

func (k ShippersKey) Delete() *spanner.Mutation {
	return spanner.Delete("shippers", k.SpannerKey())
}

func (ShippersKey) Order() []spansql.Order {
	return []spansql.Order{
		{Expr: spansql.ID("shipper_id"), Desc: false},
	}
}

func (k ShippersKey) BoolExpr() spansql.BoolExpr {
	cmp0 := spansql.BoolExpr(spansql.ComparisonOp{
		Op:  spansql.Eq,
		LHS: spansql.ID("shipper_id"),
		RHS: spansql.StringLiteral(k.ShipperId),
	})
	b := cmp0
	return spansql.Paren{Expr: b}
}

type ShipmentsKey struct {
	ShipperId  string
	ShipmentId string
}

func (k ShipmentsKey) SpannerKey() spanner.Key {
	return spanner.Key{
		k.ShipperId,
		k.ShipmentId,
	}
}

func (k ShipmentsKey) SpannerKeySet() spanner.KeySet {
	return k.SpannerKey()
}

func (k ShipmentsKey) Delete() *spanner.Mutation {
	return spanner.Delete("shipments", k.SpannerKey())
}

func (ShipmentsKey) Order() []spansql.Order {
	return []spansql.Order{
		{Expr: spansql.ID("shipper_id"), Desc: false},
		{Expr: spansql.ID("shipment_id"), Desc: false},
	}
}

func (k ShipmentsKey) BoolExpr() spansql.BoolExpr {
	cmp0 := spansql.BoolExpr(spansql.ComparisonOp{
		Op:  spansql.Eq,
		LHS: spansql.ID("shipper_id"),
		RHS: spansql.StringLiteral(k.ShipperId),
	})
	cmp1 := spansql.BoolExpr(spansql.ComparisonOp{
		Op:  spansql.Eq,
		LHS: spansql.ID("shipment_id"),
		RHS: spansql.StringLiteral(k.ShipmentId),
	})
	b := cmp0
	b = spansql.LogicalOp{
		Op:  spansql.And,
		LHS: b,
		RHS: cmp1,
	}
	return spansql.Paren{Expr: b}
}

type ShippersRowIterator interface {
	Next() (*ShippersRow, error)
	Do(f func(row *ShippersRow) error) error
	Stop()
	Count() int64
}

type streamingShippersRowIterator struct {
	*spanner.RowIterator
}

func (i *streamingShippersRowIterator) Next() (*ShippersRow, error) {
	spannerRow, err := i.RowIterator.Next()
	if err != nil {
		return nil, err
	}
	var row ShippersRow
	if err := row.UnmarshalSpannerRow(spannerRow); err != nil {
		return nil, err
	}
	return &row, nil
}

func (i *streamingShippersRowIterator) Do(f func(row *ShippersRow) error) error {
	return i.RowIterator.Do(func(spannerRow *spanner.Row) error {
		var row ShippersRow
		if err := row.UnmarshalSpannerRow(spannerRow); err != nil {
			return err
		}
		return f(&row)
	})
}

func (i *streamingShippersRowIterator) Count() int64 {
	return i.RowCount
}

type bufferedShippersRowIterator struct {
	rows []*ShippersRow
	err  error
}

func (i *bufferedShippersRowIterator) Next() (*ShippersRow, error) {
	if i.err != nil {
		return nil, i.err
	}
	if len(i.rows) == 0 {
		return nil, iterator.Done
	}
	next := i.rows[0]
	i.rows = i.rows[1:]
	return next, nil
}

func (i *bufferedShippersRowIterator) Count() int64 {
	return int64(len(i.rows))
}

func (i *bufferedShippersRowIterator) Do(f func(row *ShippersRow) error) error {
	for {
		row, err := i.Next()
		switch err {
		case iterator.Done:
			return nil
		case nil:
			if err = f(row); err != nil {
				return err
			}
		default:
			return err
		}
	}
}

func (i *bufferedShippersRowIterator) Stop() {}

type ShipmentsRowIterator interface {
	Next() (*ShipmentsRow, error)
	Do(f func(row *ShipmentsRow) error) error
	Stop()
	Count() int64
}

type streamingShipmentsRowIterator struct {
	*spanner.RowIterator
}

func (i *streamingShipmentsRowIterator) Next() (*ShipmentsRow, error) {
	spannerRow, err := i.RowIterator.Next()
	if err != nil {
		return nil, err
	}
	var row ShipmentsRow
	if err := row.UnmarshalSpannerRow(spannerRow); err != nil {
		return nil, err
	}
	return &row, nil
}

func (i *streamingShipmentsRowIterator) Do(f func(row *ShipmentsRow) error) error {
	return i.RowIterator.Do(func(spannerRow *spanner.Row) error {
		var row ShipmentsRow
		if err := row.UnmarshalSpannerRow(spannerRow); err != nil {
			return err
		}
		return f(&row)
	})
}

func (i *streamingShipmentsRowIterator) Count() int64 {
	return i.RowCount
}

type bufferedShipmentsRowIterator struct {
	rows []*ShipmentsRow
	err  error
}

func (i *bufferedShipmentsRowIterator) Next() (*ShipmentsRow, error) {
	if i.err != nil {
		return nil, i.err
	}
	if len(i.rows) == 0 {
		return nil, iterator.Done
	}
	next := i.rows[0]
	i.rows = i.rows[1:]
	return next, nil
}

func (i *bufferedShipmentsRowIterator) Count() int64 {
	return int64(len(i.rows))
}

func (i *bufferedShipmentsRowIterator) Do(f func(row *ShipmentsRow) error) error {
	for {
		row, err := i.Next()
		switch err {
		case iterator.Done:
			return nil
		case nil:
			if err = f(row); err != nil {
				return err
			}
		default:
			return err
		}
	}
}

func (i *bufferedShipmentsRowIterator) Stop() {}

type ReadTransaction struct {
	Tx SpannerReadTransaction
}

func Query(tx SpannerReadTransaction) ReadTransaction {
	return ReadTransaction{Tx: tx}
}

func (t ReadTransaction) ReadShippersRows(
	ctx context.Context,
	keySet spanner.KeySet,
) ShippersRowIterator {
	return &streamingShippersRowIterator{
		RowIterator: t.Tx.Read(
			ctx,
			"shippers",
			keySet,
			((*ShippersRow)(nil)).ColumnNames(),
		),
	}
}

type GetShippersRowQuery struct {
	Key       ShippersKey
	Shipments bool
}

func (q *GetShippersRowQuery) hasInterleavedTables() bool {
	return q.Shipments
}

func (t ReadTransaction) GetShippersRow(
	ctx context.Context,
	query GetShippersRowQuery,
) (*ShippersRow, error) {
	spannerRow, err := t.Tx.ReadRow(
		ctx,
		"shippers",
		query.Key.SpannerKey(),
		((*ShippersRow)(nil)).ColumnNames(),
	)
	if err != nil {
		return nil, err
	}
	var row ShippersRow
	if err := row.UnmarshalSpannerRow(spannerRow); err != nil {
		return nil, err
	}
	if !query.hasInterleavedTables() {
		return &row, nil
	}
	interleaved, err := t.readInterleavedShippersRows(ctx, readInterleavedShippersRowsQuery{
		KeySet:    row.Key().SpannerKey().AsPrefix(),
		Shipments: query.Shipments,
	})
	if err != nil {
		return nil, err
	}
	if rs, ok := interleaved.Shipments[row.Key()]; ok {
		row.Shipments = rs
	}
	return &row, nil
}

type BatchGetShippersRowsQuery struct {
	Keys      []ShippersKey
	Shipments bool
}

func (q *BatchGetShippersRowsQuery) hasInterleavedTables() bool {
	return q.Shipments
}

func (t ReadTransaction) BatchGetShippersRows(
	ctx context.Context,
	query BatchGetShippersRowsQuery,
) (map[ShippersKey]*ShippersRow, error) {
	spannerKeys := make([]spanner.KeySet, 0, len(query.Keys))
	spannerPrefixKeys := make([]spanner.KeySet, 0, len(query.Keys))
	for _, key := range query.Keys {
		spannerKeys = append(spannerKeys, key.SpannerKey())
		spannerPrefixKeys = append(spannerPrefixKeys, key.SpannerKey().AsPrefix())
	}
	foundRows := make(map[ShippersKey]*ShippersRow, len(query.Keys))
	if err := t.ReadShippersRows(ctx, spanner.KeySets(spannerKeys...)).Do(func(row *ShippersRow) error {
		foundRows[row.Key()] = row
		return nil
	}); err != nil {
		return nil, err
	}
	if !query.hasInterleavedTables() {
		return foundRows, nil
	}
	interleaved, err := t.readInterleavedShippersRows(ctx, readInterleavedShippersRowsQuery{
		KeySet:    spanner.KeySets(spannerPrefixKeys...),
		Shipments: query.Shipments,
	})
	if err != nil {
		return nil, err
	}
	for _, row := range foundRows {
		if rs, ok := interleaved.Shipments[row.Key()]; ok {
			row.Shipments = rs
		}
	}
	return foundRows, nil
}

type ListShippersRowsQuery struct {
	Where       spansql.BoolExpr
	Order       []spansql.Order
	Limit       int32
	Offset      int64
	Params      map[string]interface{}
	ShowDeleted bool
	Shipments   bool
}

func (q *ListShippersRowsQuery) hasInterleavedTables() bool {
	return q.Shipments
}

func (t ReadTransaction) ListShippersRows(
	ctx context.Context,
	query ListShippersRowsQuery,
) ShippersRowIterator {
	if len(query.Order) == 0 {
		query.Order = ShippersKey{}.Order()
	}
	params := make(map[string]interface{}, len(query.Params)+2)
	params["__limit"] = int64(query.Limit)
	params["__offset"] = int64(query.Offset)
	for param, value := range query.Params {
		if _, ok := params[param]; ok {
			panic(fmt.Errorf("invalid param: %s", param))
		}
		params[param] = value
	}
	if query.Where == nil {
		query.Where = spansql.True
	}
	if !query.ShowDeleted {
		query.Where = spansql.LogicalOp{
			Op:  spansql.And,
			LHS: spansql.Paren{Expr: query.Where},
			RHS: spansql.IsOp{
				LHS: spansql.ID("delete_time"),
				RHS: spansql.Null,
			},
		}
	}
	stmt := spanner.Statement{
		SQL: spansql.Query{
			Select: spansql.Select{
				List: ((*ShippersRow)(nil)).ColumnExprs(),
				From: []spansql.SelectFrom{
					spansql.SelectFromTable{Table: "shippers"},
				},
				Where: query.Where,
			},
			Order:  query.Order,
			Limit:  spansql.Param("__limit"),
			Offset: spansql.Param("__offset"),
		}.SQL(),
		Params: params,
	}
	iter := &streamingShippersRowIterator{
		RowIterator: t.Tx.Query(ctx, stmt),
	}
	if !query.hasInterleavedTables() {
		return iter
	}
	rows := make([]*ShippersRow, 0, query.Limit)
	lookup := make(map[ShippersKey]*ShippersRow, query.Limit)
	prefixes := make([]spanner.KeySet, 0, query.Limit)
	if err := iter.Do(func(row *ShippersRow) error {
		k := row.Key()
		rows = append(rows, row)
		lookup[k] = row
		prefixes = append(prefixes, k.SpannerKey().AsPrefix())
		return nil
	}); err != nil {
		return &bufferedShippersRowIterator{err: err}
	}
	interleaved, err := t.readInterleavedShippersRows(ctx, readInterleavedShippersRowsQuery{
		KeySet:    spanner.KeySets(prefixes...),
		Shipments: query.Shipments,
	})
	if err != nil {
		return &bufferedShippersRowIterator{err: err}
	}
	for key, row := range lookup {
		if rs, ok := interleaved.Shipments[key]; ok {
			row.Shipments = rs
		}
	}
	return &bufferedShippersRowIterator{rows: rows}
}

type readInterleavedShippersRowsQuery struct {
	KeySet    spanner.KeySet
	Shipments bool
}

type readInterleavedShippersRowsResult struct {
	Shipments map[ShippersKey][]*ShipmentsRow
}

func (t ReadTransaction) readInterleavedShippersRows(
	ctx context.Context,
	query readInterleavedShippersRowsQuery,
) (*readInterleavedShippersRowsResult, error) {
	var r readInterleavedShippersRowsResult
	group, groupCtx := errgroup.WithContext(ctx)
	if query.Shipments && !reflect.DeepEqual(query.KeySet, spanner.KeySets()) {
		r.Shipments = make(map[ShippersKey][]*ShipmentsRow)
		group.Go(func() error {
			if err := t.ReadShipmentsRows(groupCtx, query.KeySet).Do(func(row *ShipmentsRow) error {
				k := ShippersKey{
					ShipperId: row.ShipperId,
				}
				r.Shipments[k] = append(r.Shipments[k], row)
				return nil
			}); err != nil {
				return err
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t ReadTransaction) ReadShipmentsRows(
	ctx context.Context,
	keySet spanner.KeySet,
) ShipmentsRowIterator {
	return &streamingShipmentsRowIterator{
		RowIterator: t.Tx.Read(
			ctx,
			"shipments",
			keySet,
			((*ShipmentsRow)(nil)).ColumnNames(),
		),
	}
}

type GetShipmentsRowQuery struct {
	Key ShipmentsKey
}

func (t ReadTransaction) GetShipmentsRow(
	ctx context.Context,
	query GetShipmentsRowQuery,
) (*ShipmentsRow, error) {
	spannerRow, err := t.Tx.ReadRow(
		ctx,
		"shipments",
		query.Key.SpannerKey(),
		((*ShipmentsRow)(nil)).ColumnNames(),
	)
	if err != nil {
		return nil, err
	}
	var row ShipmentsRow
	if err := row.UnmarshalSpannerRow(spannerRow); err != nil {
		return nil, err
	}
	return &row, nil
}

type BatchGetShipmentsRowsQuery struct {
	Keys []ShipmentsKey
}

func (t ReadTransaction) BatchGetShipmentsRows(
	ctx context.Context,
	query BatchGetShipmentsRowsQuery,
) (map[ShipmentsKey]*ShipmentsRow, error) {
	spannerKeys := make([]spanner.KeySet, 0, len(query.Keys))
	spannerPrefixKeys := make([]spanner.KeySet, 0, len(query.Keys))
	for _, key := range query.Keys {
		spannerKeys = append(spannerKeys, key.SpannerKey())
		spannerPrefixKeys = append(spannerPrefixKeys, key.SpannerKey().AsPrefix())
	}
	foundRows := make(map[ShipmentsKey]*ShipmentsRow, len(query.Keys))
	if err := t.ReadShipmentsRows(ctx, spanner.KeySets(spannerKeys...)).Do(func(row *ShipmentsRow) error {
		foundRows[row.Key()] = row
		return nil
	}); err != nil {
		return nil, err
	}
	return foundRows, nil
}

type ListShipmentsRowsQuery struct {
	Where       spansql.BoolExpr
	Order       []spansql.Order
	Limit       int32
	Offset      int64
	Params      map[string]interface{}
	ShowDeleted bool
}

func (t ReadTransaction) ListShipmentsRows(
	ctx context.Context,
	query ListShipmentsRowsQuery,
) ShipmentsRowIterator {
	if len(query.Order) == 0 {
		query.Order = ShipmentsKey{}.Order()
	}
	params := make(map[string]interface{}, len(query.Params)+2)
	params["__limit"] = int64(query.Limit)
	params["__offset"] = int64(query.Offset)
	for param, value := range query.Params {
		if _, ok := params[param]; ok {
			panic(fmt.Errorf("invalid param: %s", param))
		}
		params[param] = value
	}
	if query.Where == nil {
		query.Where = spansql.True
	}
	if !query.ShowDeleted {
		query.Where = spansql.LogicalOp{
			Op:  spansql.And,
			LHS: spansql.Paren{Expr: query.Where},
			RHS: spansql.IsOp{
				LHS: spansql.ID("delete_time"),
				RHS: spansql.Null,
			},
		}
	}
	stmt := spanner.Statement{
		SQL: spansql.Query{
			Select: spansql.Select{
				List: ((*ShipmentsRow)(nil)).ColumnExprs(),
				From: []spansql.SelectFrom{
					spansql.SelectFromTable{Table: "shipments"},
				},
				Where: query.Where,
			},
			Order:  query.Order,
			Limit:  spansql.Param("__limit"),
			Offset: spansql.Param("__offset"),
		}.SQL(),
		Params: params,
	}
	iter := &streamingShipmentsRowIterator{
		RowIterator: t.Tx.Query(ctx, stmt),
	}
	return iter
}

type SpannerReadTransaction interface {
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	ReadUsingIndex(ctx context.Context, table, index string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
	ReadRowUsingIndex(ctx context.Context, table string, index string, key spanner.Key, columns []string) (*spanner.Row, error)
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}
