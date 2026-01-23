package ch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

// Environment variables for ClickHouse connection:
//
//	CH_HOST     - ClickHouse host (default: localhost)
//	CH_PORT     - ClickHouse port (default: 9000)
//	CH_USER     - Username (default: default)
//	CH_PASSWORD - Password (default: empty)
//	CH_DATABASE - Database (default: default)

func envOr(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// skipIfNoClickHouse skips the test if CH_HOST is not set.
func skipIfNoClickHouse(tb testing.TB) {
	tb.Helper()
	if os.Getenv("CH_HOST") == "" {
		tb.Skip("CH_HOST not set - skipping (need ClickHouse connection)")
	}
}

// getTestClient returns a connected ClickHouse client using environment variables.
// Skips the test if CH_HOST is not set.
func getTestClient(tb testing.TB) *Client {
	tb.Helper()
	skipIfNoClickHouse(tb)

	ctx := context.Background()
	addr := fmt.Sprintf("%s:%s", envOr("CH_HOST", "localhost"), envOr("CH_PORT", "9000"))

	settings := []Setting{
		{Key: "allow_experimental_object_type", Value: "1", Important: true},
		{Key: "output_format_native_write_json_as_string", Value: "1", Important: true},
	}

	client, err := Dial(ctx, Options{
		Address:  addr,
		User:     envOr("CH_USER", "default"),
		Password: envOr("CH_PASSWORD", ""),
		Database: envOr("CH_DATABASE", "default"),
		Settings: settings,
	})
	require.NoError(tb, err, "failed to connect to ClickHouse at %s", addr)

	tb.Cleanup(func() { _ = client.Close() })
	return client
}

func TestNestedBasic(t *testing.T) {
	ctx := context.Background()
	conn := getTestClient(t)

	// Create table with Nested column
	require.NoError(t, conn.Do(ctx, Query{
		Body: `CREATE TABLE IF NOT EXISTS test_nested (
			id UInt64,
			events Nested(
				event_id UInt32,
				event_name String
			)
		) ENGINE = Memory`,
	}))
	t.Cleanup(func() {
		_ = conn.Do(ctx, Query{Body: "DROP TABLE IF EXISTS test_nested"})
	})

	// Truncate for clean test
	_ = conn.Do(ctx, Query{Body: "TRUNCATE TABLE test_nested"})

	// Prepare data for INSERT
	// Nested columns are sent as separate Array columns with dot notation
	eventIds := new(proto.ColUInt32).Array()
	eventNames := new(proto.ColStr).Array()

	// Row 1: events = [(1, "click"), (2, "view")]
	eventIds.Append([]uint32{1, 2})
	eventNames.Append([]string{"click", "view"})

	// Row 2: events = [(3, "purchase")]
	eventIds.Append([]uint32{3})
	eventNames.Append([]string{"purchase"})

	// Row 3: events = [] (empty)
	eventIds.Append([]uint32{})
	eventNames.Append([]string{})

	// INSERT using flattened columns
	require.NoError(t, conn.Do(ctx, Query{
		Body: "INSERT INTO test_nested VALUES",
		Input: proto.Input{
			{Name: "id", Data: proto.ColUInt64{1, 2, 3}},
			{Name: "events.event_id", Data: eventIds},
			{Name: "events.event_name", Data: eventNames},
		},
	}))

	// SELECT and verify
	var (
		resultId         proto.ColUInt64
		resultEventIds   = new(proto.ColUInt32).Array()
		resultEventNames = new(proto.ColStr).Array()
	)

	require.NoError(t, conn.Do(ctx, Query{
		Body: "SELECT id, `events.event_id`, `events.event_name` FROM test_nested ORDER BY id",
		Result: proto.Results{
			{Name: "id", Data: &resultId},
			{Name: "events.event_id", Data: resultEventIds},
			{Name: "events.event_name", Data: resultEventNames},
		},
	}))

	// Verify results
	require.Equal(t, 3, resultId.Rows())
	require.Equal(t, 3, resultEventIds.Rows())
	require.Equal(t, 3, resultEventNames.Rows())

	// Row 1
	assert.Equal(t, uint64(1), resultId.Row(0))
	assert.Equal(t, []uint32{1, 2}, resultEventIds.Row(0))
	assert.Equal(t, []string{"click", "view"}, resultEventNames.Row(0))

	// Row 2
	assert.Equal(t, uint64(2), resultId.Row(1))
	assert.Equal(t, []uint32{3}, resultEventIds.Row(1))
	assert.Equal(t, []string{"purchase"}, resultEventNames.Row(1))

	// Row 3 (empty) - empty arrays may be returned as nil
	assert.Equal(t, uint64(3), resultId.Row(2))
	assert.Empty(t, resultEventIds.Row(2))
	assert.Empty(t, resultEventNames.Row(2))
}

func TestNestedWithHelper(t *testing.T) {
	ctx := context.Background()
	conn := getTestClient(t)

	// Create table with Nested column
	require.NoError(t, conn.Do(ctx, Query{
		Body: `CREATE TABLE IF NOT EXISTS test_nested_helper (
			id UInt64,
			tags Nested(
				name String,
				value Float64
			)
		) ENGINE = Memory`,
	}))
	t.Cleanup(func() {
		_ = conn.Do(ctx, Query{Body: "DROP TABLE IF EXISTS test_nested_helper"})
	})

	_ = conn.Do(ctx, Query{Body: "TRUNCATE TABLE test_nested_helper"})

	// Use ColNested helper
	nested := proto.NewNested(
		proto.NestedColumn{Name: "name", Data: new(proto.ColStr).Array()},
		proto.NestedColumn{Name: "value", Data: new(proto.ColFloat64).Array()},
	)

	// Append rows using the helper
	require.NoError(t, nested.Append(map[string]any{
		"name":  []string{"tag1", "tag2"},
		"value": []float64{1.5, 2.5},
	}))
	require.NoError(t, nested.Append(map[string]any{
		"name":  []string{"single"},
		"value": []float64{3.5},
	}))

	// INSERT using InputColumns helper
	idCol := proto.ColUInt64{100, 200}
	input := proto.Input{{Name: "id", Data: idCol}}
	input = append(input, nested.InputColumns("tags")...)

	require.NoError(t, conn.Do(ctx, Query{
		Body:  "INSERT INTO test_nested_helper VALUES",
		Input: input,
	}))

	// SELECT using ResultColumns helper
	resultNested := proto.NewNested(
		proto.NestedColumn{Name: "name", Data: new(proto.ColStr).Array()},
		proto.NestedColumn{Name: "value", Data: new(proto.ColFloat64).Array()},
	)

	var resultId proto.ColUInt64
	results := proto.Results{{Name: "id", Data: &resultId}}
	results = append(results, resultNested.ResultColumns("tags")...)

	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT id, `tags.name`, `tags.value` FROM test_nested_helper ORDER BY id",
		Result: results,
	}))

	// Verify
	require.Equal(t, 2, resultId.Rows())
	assert.Equal(t, uint64(100), resultId.Row(0))
	assert.Equal(t, uint64(200), resultId.Row(1))

	// Access data through the nested helper
	nameCol := resultNested.Column("name")
	require.NotNil(t, nameCol)
	nameArr := nameCol.Data.(*proto.ColArr[string])
	assert.Equal(t, []string{"tag1", "tag2"}, nameArr.Row(0))
	assert.Equal(t, []string{"single"}, nameArr.Row(1))
}

func TestNestedMultipleColumns(t *testing.T) {
	ctx := context.Background()
	conn := getTestClient(t)

	// Create table with multiple Nested columns
	require.NoError(t, conn.Do(ctx, Query{
		Body: `CREATE TABLE IF NOT EXISTS test_nested_multi (
			id UInt64,
			users Nested(
				user_id UInt32,
				name String
			),
			events Nested(
				event_type String,
				timestamp Int64
			)
		) ENGINE = Memory`,
	}))
	t.Cleanup(func() {
		_ = conn.Do(ctx, Query{Body: "DROP TABLE IF EXISTS test_nested_multi"})
	})

	_ = conn.Do(ctx, Query{Body: "TRUNCATE TABLE test_nested_multi"})

	// Prepare users nested
	usersIds := new(proto.ColUInt32).Array()
	usersNames := new(proto.ColStr).Array()
	usersIds.Append([]uint32{1, 2})
	usersNames.Append([]string{"alice", "bob"})

	// Prepare events nested
	eventTypes := new(proto.ColStr).Array()
	eventTimestamps := new(proto.ColInt64).Array()
	eventTypes.Append([]string{"login", "click", "logout"})
	eventTimestamps.Append([]int64{1000, 2000, 3000})

	require.NoError(t, conn.Do(ctx, Query{
		Body: "INSERT INTO test_nested_multi VALUES",
		Input: proto.Input{
			{Name: "id", Data: proto.ColUInt64{1}},
			{Name: "users.user_id", Data: usersIds},
			{Name: "users.name", Data: usersNames},
			{Name: "events.event_type", Data: eventTypes},
			{Name: "events.timestamp", Data: eventTimestamps},
		},
	}))

	// SELECT and verify
	var (
		resultId              proto.ColUInt64
		resultUsersIds        = new(proto.ColUInt32).Array()
		resultUsersNames      = new(proto.ColStr).Array()
		resultEventTypes      = new(proto.ColStr).Array()
		resultEventTimestamps = new(proto.ColInt64).Array()
	)

	require.NoError(t, conn.Do(ctx, Query{
		Body: "SELECT * FROM test_nested_multi",
		Result: proto.Results{
			{Name: "id", Data: &resultId},
			{Name: "users.user_id", Data: resultUsersIds},
			{Name: "users.name", Data: resultUsersNames},
			{Name: "events.event_type", Data: resultEventTypes},
			{Name: "events.timestamp", Data: resultEventTimestamps},
		},
	}))

	require.Equal(t, 1, resultId.Rows())
	assert.Equal(t, []uint32{1, 2}, resultUsersIds.Row(0))
	assert.Equal(t, []string{"alice", "bob"}, resultUsersNames.Row(0))
	assert.Equal(t, []string{"login", "click", "logout"}, resultEventTypes.Row(0))
	assert.Equal(t, []int64{1000, 2000, 3000}, resultEventTimestamps.Row(0))
}

func TestNestedTypeInference(t *testing.T) {
	// Test that ColNested.Infer works correctly
	nested := &proto.ColNested{}
	require.NoError(t, nested.Infer("Nested(id UInt64, name String, value Float64)"))

	assert.Equal(t, "Nested(id UInt64, name String, value Float64)", nested.Type().String())

	cols := nested.Columns()
	require.Len(t, cols, 3)
	assert.Equal(t, "id", cols[0].Name)
	assert.Equal(t, "name", cols[1].Name)
	assert.Equal(t, "value", cols[2].Name)

	// Verify internal types are Array(T)
	assert.Equal(t, "Array(UInt64)", cols[0].Data.Type().String())
	assert.Equal(t, "Array(String)", cols[1].Data.Type().String())
	assert.Equal(t, "Array(Float64)", cols[2].Data.Type().String())
}

func TestNestedLargeData(t *testing.T) {
	ctx := context.Background()
	conn := getTestClient(t)

	require.NoError(t, conn.Do(ctx, Query{
		Body: `CREATE TABLE IF NOT EXISTS test_nested_large (
			id UInt64,
			data Nested(
				idx UInt32,
				val String
			)
		) ENGINE = Memory`,
	}))
	t.Cleanup(func() {
		_ = conn.Do(ctx, Query{Body: "DROP TABLE IF EXISTS test_nested_large"})
	})

	_ = conn.Do(ctx, Query{Body: "TRUNCATE TABLE test_nested_large"})

	// Generate large data
	const numRows = 1000
	const elementsPerRow = 100

	idCol := make(proto.ColUInt64, numRows)
	idxCol := new(proto.ColUInt32).Array()
	valCol := new(proto.ColStr).Array()

	for i := range numRows {
		idCol[i] = uint64(i)

		idxs := make([]uint32, elementsPerRow)
		vals := make([]string, elementsPerRow)
		for j := range elementsPerRow {
			idxs[j] = uint32(j)
			vals[j] = "value"
		}
		idxCol.Append(idxs)
		valCol.Append(vals)
	}

	require.NoError(t, conn.Do(ctx, Query{
		Body: "INSERT INTO test_nested_large VALUES",
		Input: proto.Input{
			{Name: "id", Data: idCol},
			{Name: "data.idx", Data: idxCol},
			{Name: "data.val", Data: valCol},
		},
	}))

	// Verify count
	var count proto.ColUInt64
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT count() FROM test_nested_large",
		Result: proto.Results{{Name: "count()", Data: &count}},
	}))
	assert.Equal(t, uint64(numRows), count.Row(0))

	// Verify array lengths
	var (
		resultIdxCol = new(proto.ColUInt32).Array()
	)
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT `data.idx` FROM test_nested_large LIMIT 1",
		Result: proto.Results{{Name: "data.idx", Data: resultIdxCol}},
	}))
	assert.Equal(t, elementsPerRow, len(resultIdxCol.Row(0)))
}

// =============================================================================
// BENCHMARKS
// =============================================================================
// These benchmarks test encoding/decoding performance at the protocol level,
// following the same pattern as other benchmarks in proto/*_test.go.
// They do NOT require a ClickHouse server.

// BenchmarkColNested_EncodeColumn benchmarks encoding nested column data.
// Tests encoding of multiple parallel Array columns (how Nested is stored).
func BenchmarkColNested_EncodeColumn(b *testing.B) {
	const rows = 1_000
	const elementsPerRow = 10

	// Prepare data: Nested(id UInt64, name String, value Float64)
	nestedIds := new(proto.ColUInt64).Array()
	nestedNames := new(proto.ColStr).Array()
	nestedValues := new(proto.ColFloat64).Array()

	for i := range rows {
		rowIds := make([]uint64, elementsPerRow)
		rowNames := make([]string, elementsPerRow)
		rowValues := make([]float64, elementsPerRow)

		for j := range elementsPerRow {
			rowIds[j] = uint64(i*elementsPerRow + j)
			rowNames[j] = fmt.Sprintf("item_%d_%d", i, j)
			rowValues[j] = float64(j) * 1.5
		}

		nestedIds.Append(rowIds)
		nestedNames.Append(rowNames)
		nestedValues.Append(rowValues)
	}

	var buf proto.Buffer
	nestedIds.EncodeColumn(&buf)
	nestedNames.EncodeColumn(&buf)
	nestedValues.EncodeColumn(&buf)

	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		nestedIds.EncodeColumn(&buf)
		nestedNames.EncodeColumn(&buf)
		nestedValues.EncodeColumn(&buf)
	}
}

// BenchmarkColNested_DecodeColumn benchmarks decoding nested column data.
func BenchmarkColNested_DecodeColumn(b *testing.B) {
	const rows = 1_000
	const elementsPerRow = 10

	// Prepare and encode data
	nestedIds := new(proto.ColUInt64).Array()
	nestedNames := new(proto.ColStr).Array()
	nestedValues := new(proto.ColFloat64).Array()

	for i := 0; i < rows; i++ {
		rowIds := make([]uint64, elementsPerRow)
		rowNames := make([]string, elementsPerRow)
		rowValues := make([]float64, elementsPerRow)

		for j := 0; j < elementsPerRow; j++ {
			rowIds[j] = uint64(i*elementsPerRow + j)
			rowNames[j] = fmt.Sprintf("item_%d_%d", i, j)
			rowValues[j] = float64(j) * 1.5
		}

		nestedIds.Append(rowIds)
		nestedNames.Append(rowNames)
		nestedValues.Append(rowValues)
	}

	// Encode each column separately (as they would be sent over the wire)
	var bufIds, bufNames, bufValues proto.Buffer
	nestedIds.EncodeColumn(&bufIds)
	nestedNames.EncodeColumn(&bufNames)
	nestedValues.EncodeColumn(&bufValues)

	totalBytes := len(bufIds.Buf) + len(bufNames.Buf) + len(bufValues.Buf)
	b.SetBytes(int64(totalBytes))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		decIds := new(proto.ColUInt64).Array()
		decNames := new(proto.ColStr).Array()
		decValues := new(proto.ColFloat64).Array()

		rIds := proto.NewReader(bytes.NewReader(bufIds.Buf))
		rNames := proto.NewReader(bytes.NewReader(bufNames.Buf))
		rValues := proto.NewReader(bytes.NewReader(bufValues.Buf))

		if err := decIds.DecodeColumn(rIds, rows); err != nil {
			b.Fatal(err)
		}
		if err := decNames.DecodeColumn(rNames, rows); err != nil {
			b.Fatal(err)
		}
		if err := decValues.DecodeColumn(rValues, rows); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkColNested_Wide_EncodeColumn benchmarks encoding wide nested data (10 columns).
func BenchmarkColNested_Wide_EncodeColumn(b *testing.B) {
	const rows = 1_000

	// 5 UInt64 columns + 5 String columns
	arrCols := make([]*proto.ColArr[uint64], 5)
	strCols := make([]*proto.ColArr[string], 5)

	for i := range arrCols {
		arrCols[i] = new(proto.ColUInt64).Array()
		strCols[i] = new(proto.ColStr).Array()
	}

	for i := range rows {
		for j := range arrCols {
			arrCols[j].Append([]uint64{uint64(i), uint64(i + 1)})
			strCols[j].Append([]string{"a", "b"})
		}
	}

	var buf proto.Buffer
	for _, col := range arrCols {
		col.EncodeColumn(&buf)
	}
	for _, col := range strCols {
		col.EncodeColumn(&buf)
	}

	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		for _, col := range arrCols {
			col.EncodeColumn(&buf)
		}
		for _, col := range strCols {
			col.EncodeColumn(&buf)
		}
	}
}

// BenchmarkColNested_Wide_DecodeColumn benchmarks decoding wide nested data (10 columns).
func BenchmarkColNested_Wide_DecodeColumn(b *testing.B) {
	const rows = 1_000

	// Prepare data
	arrCols := make([]*proto.ColArr[uint64], 5)
	strCols := make([]*proto.ColArr[string], 5)

	for i := range arrCols {
		arrCols[i] = new(proto.ColUInt64).Array()
		strCols[i] = new(proto.ColStr).Array()
	}

	for i := 0; i < rows; i++ {
		for j := range arrCols {
			arrCols[j].Append([]uint64{uint64(i), uint64(i + 1)})
			strCols[j].Append([]string{"a", "b"})
		}
	}

	// Encode each column
	arrBufs := make([]proto.Buffer, 5)
	strBufs := make([]proto.Buffer, 5)
	var totalBytes int

	for i, col := range arrCols {
		col.EncodeColumn(&arrBufs[i])
		totalBytes += len(arrBufs[i].Buf)
	}
	for i, col := range strCols {
		col.EncodeColumn(&strBufs[i])
		totalBytes += len(strBufs[i].Buf)
	}

	b.SetBytes(int64(totalBytes))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := range arrCols {
			decArr := new(proto.ColUInt64).Array()
			decStr := new(proto.ColStr).Array()
			rArr := proto.NewReader(bytes.NewReader(arrBufs[j].Buf))
			rStr := proto.NewReader(bytes.NewReader(strBufs[j].Buf))

			if err := decArr.DecodeColumn(rArr, rows); err != nil {
				b.Fatal(err)
			}
			if err := decStr.DecodeColumn(rStr, rows); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkColNested_Deep_EncodeColumn benchmarks encoding nested data with large arrays (100 elements per row).
func BenchmarkColNested_Deep_EncodeColumn(b *testing.B) {
	const rows = 1_000
	const elementsPerRow = 100

	idxCol := new(proto.ColUInt64).Array()
	valCol := new(proto.ColStr).Array()

	for i := 0; i < rows; i++ {
		idxs := make([]uint64, elementsPerRow)
		vals := make([]string, elementsPerRow)
		for j := 0; j < elementsPerRow; j++ {
			idxs[j] = uint64(j)
			vals[j] = fmt.Sprintf("value_%d", j)
		}
		idxCol.Append(idxs)
		valCol.Append(vals)
	}

	var buf proto.Buffer
	idxCol.EncodeColumn(&buf)
	valCol.EncodeColumn(&buf)

	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		idxCol.EncodeColumn(&buf)
		valCol.EncodeColumn(&buf)
	}
}

// BenchmarkColNested_Deep_DecodeColumn benchmarks decoding nested data with large arrays (100 elements per row).
func BenchmarkColNested_Deep_DecodeColumn(b *testing.B) {
	const rows = 1_000
	const elementsPerRow = 100

	// Prepare data
	idxCol := new(proto.ColUInt64).Array()
	valCol := new(proto.ColStr).Array()

	for i := 0; i < rows; i++ {
		idxs := make([]uint64, elementsPerRow)
		vals := make([]string, elementsPerRow)
		for j := 0; j < elementsPerRow; j++ {
			idxs[j] = uint64(j)
			vals[j] = fmt.Sprintf("value_%d", j)
		}
		idxCol.Append(idxs)
		valCol.Append(vals)
	}

	// Encode
	var bufIdx, bufVal proto.Buffer
	idxCol.EncodeColumn(&bufIdx)
	valCol.EncodeColumn(&bufVal)

	totalBytes := len(bufIdx.Buf) + len(bufVal.Buf)
	b.SetBytes(int64(totalBytes))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		decIdx := new(proto.ColUInt64).Array()
		decVal := new(proto.ColStr).Array()

		rIdx := proto.NewReader(bytes.NewReader(bufIdx.Buf))
		rVal := proto.NewReader(bytes.NewReader(bufVal.Buf))

		if err := decIdx.DecodeColumn(rIdx, rows); err != nil {
			b.Fatal(err)
		}
		if err := decVal.DecodeColumn(rVal, rows); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkColStr_JSONMarshal benchmarks JSON marshaling for comparison with native encoding.
// This shows the overhead of using JSON strings vs native Nested types.
func BenchmarkColStr_JSONMarshal(b *testing.B) {
	const rows = 1_000
	const elementsPerRow = 10

	type Element struct {
		ID    uint64  `json:"id"`
		Name  string  `json:"name"`
		Value float64 `json:"value"`
	}

	type JSONData struct {
		Items []Element `json:"items"`
	}

	b.ResetTimer()
	b.ReportAllocs()

	var totalBytes int64
	for i := 0; i < b.N; i++ {
		var strCol proto.ColStr
		for row := 0; row < rows; row++ {
			elements := make([]Element, elementsPerRow)
			for j := 0; j < elementsPerRow; j++ {
				elements[j] = Element{
					ID:    uint64(row*elementsPerRow + j),
					Name:  fmt.Sprintf("item_%d_%d", row, j),
					Value: float64(j) * 1.5,
				}
			}
			data := JSONData{Items: elements}
			jsonBytes, _ := json.Marshal(data)
			strCol.Append(string(jsonBytes))
		}

		var buf proto.Buffer
		strCol.EncodeColumn(&buf)
		totalBytes = int64(len(buf.Buf))
	}
	b.SetBytes(totalBytes)
}

// BenchmarkColStr_JSONUnmarshal benchmarks JSON unmarshaling for comparison.
func BenchmarkColStr_JSONUnmarshal(b *testing.B) {
	const rows = 1_000
	const elementsPerRow = 10

	type Element struct {
		ID    uint64  `json:"id"`
		Name  string  `json:"name"`
		Value float64 `json:"value"`
	}

	type JSONData struct {
		Items []Element `json:"items"`
	}

	// Prepare encoded data
	var strCol proto.ColStr
	for row := 0; row < rows; row++ {
		elements := make([]Element, elementsPerRow)
		for j := 0; j < elementsPerRow; j++ {
			elements[j] = Element{
				ID:    uint64(row*elementsPerRow + j),
				Name:  fmt.Sprintf("item_%d_%d", row, j),
				Value: float64(j) * 1.5,
			}
		}
		data := JSONData{Items: elements}
		jsonBytes, _ := json.Marshal(data)
		strCol.Append(string(jsonBytes))
	}

	var buf proto.Buffer
	strCol.EncodeColumn(&buf)

	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r := proto.NewReader(bytes.NewReader(buf.Buf))

		var dec proto.ColStr
		if err := dec.DecodeColumn(r, rows); err != nil {
			b.Fatal(err)
		}

		// Unmarshal each row (the real cost of using JSON strings)
		for j := 0; j < dec.Rows(); j++ {
			var data JSONData
			if err := json.Unmarshal([]byte(dec.Row(j)), &data); err != nil {
				b.Fatal(err)
			}
		}
	}
}
