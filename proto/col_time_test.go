package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColTime32_DecodeColumn(t *testing.T) {
	t.Parallel()
	const rows = 50
	var data ColTime32
	for i := 0; i < rows; i++ {
		v := Time32(i)
		data.Append(v)
		require.Equal(t, v, data.Row(i))
	}
	require.Equal(t, ColumnType("Time"), data.Type())

	var buf Buffer
	data.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		t.Parallel()
		gold.Bytes(t, buf.Buf, "col_time32")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		var dec ColTime32
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnTypeTime, dec.Type())
	})
	t.Run("ColTimeAlias", func(t *testing.T) {
		var alias ColTime
		alias.Append(Time32(3723))
		alias.Append(Time32(-3723))
		require.Equal(t, ColumnTypeTime, alias.Type())
		require.Equal(t, ColumnType("Nullable(Time)"), alias.Nullable().Type())
		require.Equal(t, ColumnType("Array(Time)"), alias.Array().Type())
		require.Equal(t, ColumnType("LowCardinality(Time)"), alias.LowCardinality().Type())

		var aliasBuf Buffer
		alias.EncodeColumn(&aliasBuf)

		var dec ColTime
		require.NoError(t, dec.DecodeColumn(NewReader(bytes.NewReader(aliasBuf.Buf)), alias.Rows()))
		require.Equal(t, alias, dec)
	})
	t.Run("ZeroRows", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		var dec ColTime32
		require.NoError(t, dec.DecodeColumn(r, 0))
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		var dec ColTime32
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		var dec ColTime32
		requireNoShortRead(t, buf.Buf, colAware(&dec, rows))
	})
	t.Run("ZeroRowsEncode", func(t *testing.T) {
		var v ColTime32
		v.EncodeColumn(nil)
	})
	t.Run("WriteColumn", checkWriteColumn(data))
}

func TestColTime32_Nullable(t *testing.T) {
	t.Parallel()
	col := new(ColTime32).Nullable()
	require.Equal(t, ColumnType("Nullable(Time)"), col.Type())

	values := []Nullable[Time32]{
		NewNullable(Time32(3723)),
		Null[Time32](),
		NewNullable(Time32(-3723)),
		NewNullable(Time32(0)),
	}
	col.AppendArr(values)

	var buf Buffer
	col.EncodeColumn(&buf)

	t.Run("Ok", func(t *testing.T) {
		dec := new(ColTime).Nullable()
		require.NoError(t, dec.DecodeColumn(NewReader(bytes.NewReader(buf.Buf)), col.Rows()))
		requireEqual[Nullable[Time32]](t, col, dec)
		require.Equal(t, ColumnType("Nullable(Time)"), dec.Type())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
	})
	t.Run("EOF", func(t *testing.T) {
		dec := new(ColTime32).Nullable()
		require.ErrorIs(t, dec.DecodeColumn(NewReader(bytes.NewReader(nil)), col.Rows()), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := new(ColTime32).Nullable()
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
	t.Run("WriteColumn", checkWriteColumn(col))
}

func TestColTime32_Array(t *testing.T) {
	t.Parallel()
	col := NewArrTime32()
	require.Equal(t, ColumnType("Array(Time)"), col.Type())

	col.AppendArr([][]Time32{
		{0, 3600},
		{},
		{-3723, 86399},
	})

	var buf Buffer
	col.EncodeColumn(&buf)

	t.Run("Ok", func(t *testing.T) {
		dec := new(ColTime).Array()
		require.NoError(t, dec.DecodeColumn(NewReader(bytes.NewReader(buf.Buf)), col.Rows()))
		requireEqual[[]Time32](t, col, dec)
		require.Equal(t, ColumnType("Array(Time)"), dec.Type())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
	})
	t.Run("EOF", func(t *testing.T) {
		dec := NewArrTime32()
		require.ErrorIs(t, dec.DecodeColumn(NewReader(bytes.NewReader(nil)), col.Rows()), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := NewArrTime32()
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
	t.Run("WriteColumn", checkWriteColumn(col))
}

func TestColTime32_LowCardinality(t *testing.T) {
	t.Parallel()
	col := new(ColTime32).LowCardinality()
	require.Equal(t, ColumnType("LowCardinality(Time)"), col.Type())

	col.AppendArr([]Time32{60, -60, 60})
	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)

	t.Run("Ok", func(t *testing.T) {
		dec := new(ColTime).LowCardinality()
		require.NoError(t, dec.DecodeColumn(NewReader(bytes.NewReader(buf.Buf)), col.Rows()))
		requireEqual[Time32](t, col, dec)
		require.Equal(t, ColumnType("LowCardinality(Time)"), dec.Type())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
	})
	t.Run("EOF", func(t *testing.T) {
		dec := new(ColTime32).LowCardinality()
		require.ErrorIs(t, dec.DecodeColumn(NewReader(bytes.NewReader(nil)), col.Rows()), io.EOF)
	})
	t.Run("WriteColumn", checkWriteColumn(col))
}

func TestColTime32_BlockRoundTrip(t *testing.T) {
	t.Parallel()

	tCol := &ColTime32{Data: []Time32{3723, -3723}}
	aliasCol := &ColTime{Data: []Time32{0, 86399}}
	nullCol := new(ColTime32).Nullable()
	nullCol.Append(NewNullable(Time32(120)))
	nullCol.Append(Null[Time32]())
	arrCol := NewArrTime32()
	arrCol.Append([]Time32{1, 2})
	arrCol.Append([]Time32{-3})
	lcCol := new(ColTime32).LowCardinality()
	lcCol.AppendArr([]Time32{10, 10})

	inputs := []InputColumn{
		{Name: "t", Data: tCol},
		{Name: "a", Data: aliasCol},
		{Name: "n", Data: nullCol},
		{Name: "arr", Data: arrCol},
		{Name: "lc", Data: lcCol},
	}

	var metaBuf Buffer
	require.NoError(t, (Block{Columns: len(inputs)}).EncodeBlock(&metaBuf, Version, []InputColumn{
		{Name: "t", Data: new(ColTime32)},
		{Name: "a", Data: new(ColTime)},
		{Name: "n", Data: new(ColTime32).Nullable()},
		{Name: "arr", Data: NewArrTime32()},
		{Name: "lc", Data: new(ColTime32).LowCardinality()},
	}))
	var metaBlock Block
	var info ColInfoInput
	require.NoError(t, metaBlock.DecodeBlock(metaBuf.Reader(), Version, &info))
	require.Equal(t, ColInfoInput{
		{Name: "t", Type: ColumnType("Time")},
		{Name: "a", Type: ColumnType("Time")},
		{Name: "n", Type: ColumnType("Nullable(Time)")},
		{Name: "arr", Type: ColumnType("Array(Time)")},
		{Name: "lc", Type: ColumnType("LowCardinality(Time)")},
	}, info)

	var blockBuf Buffer
	require.NoError(t, (Block{Columns: len(inputs), Rows: 2}).EncodeBlock(&blockBuf, Version, inputs))

	t.Run("ExplicitResults", func(t *testing.T) {
		var (
			decBlock Block
			gotT     ColTime32
			gotA     ColTime
			gotN     = new(ColTime32).Nullable()
			gotArr   = NewArrTime32()
			gotLC    = new(ColTime32).LowCardinality()
		)
		require.NoError(t, decBlock.DecodeBlock(blockBuf.Reader(), Version, Results{
			{Name: "t", Data: &gotT},
			{Name: "a", Data: &gotA},
			{Name: "n", Data: gotN},
			{Name: "arr", Data: gotArr},
			{Name: "lc", Data: gotLC},
		}))
		require.Equal(t, *tCol, gotT)
		require.Equal(t, *aliasCol, gotA)
		requireEqual[Nullable[Time32]](t, nullCol, gotN)
		requireEqual[[]Time32](t, arrCol, gotArr)
		requireEqual[Time32](t, lcCol, gotLC)
	})

	t.Run("ColAuto", func(t *testing.T) {
		autoInputs := []InputColumn{
			{Name: "t", Data: tCol},
			{Name: "a", Data: aliasCol},
			{Name: "n", Data: nullCol},
			{Name: "arr", Data: arrCol},
		}
		var autoBuf Buffer
		require.NoError(t, (Block{Columns: len(autoInputs), Rows: 2}).EncodeBlock(&autoBuf, Version, autoInputs))

		var (
			decBlock Block
			autoT    ColAuto
			autoA    ColAuto
			autoN    ColAuto
			autoArr  ColAuto
		)
		res := Results{
			{Name: "t", Data: &autoT},
			{Name: "a", Data: &autoA},
			{Name: "n", Data: &autoN},
			{Name: "arr", Data: &autoArr},
		}
		require.NoError(t, decBlock.DecodeBlock(autoBuf.Reader(), Version, res))
		require.Equal(t, ColumnType("Time"), autoT.Type())
		require.Equal(t, ColumnType("Time"), autoA.Type())
		require.Equal(t, ColumnType("Nullable(Time)"), autoN.Type())
		require.Equal(t, ColumnType("Array(Time)"), autoArr.Type())

		gotT, ok := autoT.Data.(*ColTime32)
		require.True(t, ok)
		require.Equal(t, *tCol, *gotT)

		// Second block reuses the inferred columns.
		require.NoError(t, decBlock.DecodeBlock(autoBuf.Reader(), Version, res))
		require.Equal(t, *tCol, *gotT)
	})

	t.Run("ResultsAuto", func(t *testing.T) {
		var res Results
		var decBlock Block
		require.NoError(t, decBlock.DecodeBlock(blockBuf.Reader(), Version, res.Auto()))
		require.Len(t, res, len(inputs))
		require.Equal(t, ColumnType("Time"), res[0].Data.Type())
		require.Equal(t, ColumnType("Time"), res[1].Data.Type())
		require.Equal(t, ColumnType("Nullable(Time)"), res[2].Data.Type())
		require.Equal(t, ColumnType("Array(Time)"), res[3].Data.Type())
		require.Equal(t, ColumnType("LowCardinality(Time)"), res[4].Data.Type())

		// Second block reuses the inferred columns.
		require.NoError(t, decBlock.DecodeBlock(blockBuf.Reader(), Version, res.Auto()))
		gotT, ok := res[0].Data.(*ColTime32)
		require.True(t, ok)
		require.Equal(t, *tCol, *gotT)
	})

	t.Run("RejectTime32WireType", func(t *testing.T) {
		var badBuf Buffer
		badBlock := Block{Columns: 1, Rows: 1}
		badBlock.EncodeAware(&badBuf, Version)
		badBuf.PutString("t")
		badBuf.PutString("Time32")
		if FeatureCustomSerialization.In(Version) {
			badBuf.PutBool(false)
		}
		badBuf.PutInt32(0)

		var decBlock Block
		err := decBlock.DecodeBlock(badBuf.Reader(), Version, Results{
			{Name: "t", Data: new(ColTime32)},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), `unexpected type "Time32" (got) instead of "Time" (has)`)
	})
}
