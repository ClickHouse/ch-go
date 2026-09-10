package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

// newTestVariant returns Variant(Int64, String) and its columns.
func newTestVariant() (*ColVariant, *ColInt64, *ColStr) {
	var (
		ints = new(ColInt64)
		strs = new(ColStr)
	)
	return NewVariant(ints, strs), ints, strs
}

func TestColVariantGolden(t *testing.T) {
	v, ints, strs := newTestVariant()
	require.Equal(t, ColumnType("Variant(Int64, String)"), v.Type())

	v.AppendDiscriminator(0)
	ints.Append(42)
	v.AppendDiscriminator(1)
	strs.Append("foo")
	v.AppendNull()
	v.AppendDiscriminator(0)
	ints.Append(100)

	var buf Buffer
	v.EncodeState(&buf)
	v.EncodeColumn(&buf)
	gold.Bytes(t, buf.Buf, "col_variant_of_int64_str")
}

func TestColVariant(t *testing.T) {
	v, ints, strs := newTestVariant()

	v.AppendDiscriminator(0)
	ints.Append(42)
	v.AppendDiscriminator(1)
	strs.Append("foo")
	v.AppendNull()
	v.AppendDiscriminator(0)
	ints.Append(100)
	const rows = 4

	require.Equal(t, rows, v.Rows())
	require.True(t, v.RowIsNull(2))
	require.False(t, v.RowIsNull(0))
	require.Equal(t, 1, v.RowOffset(3))

	d, ok := v.Discriminator(ColumnTypeString)
	require.True(t, ok)
	require.Equal(t, uint8(1), d)
	_, ok = v.Discriminator(ColumnTypeUInt8)
	require.False(t, ok)

	var buf Buffer
	v.EncodeColumn(&buf)

	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec, decInts, decStrs := newTestVariant()
		require.NoError(t, dec.DecodeColumn(r, rows))

		require.Equal(t, []uint8{0, 1, VariantNullDiscriminator, 0}, []uint8(dec.Discriminators))
		require.Equal(t, int64(42), decInts.Row(dec.RowOffset(0)))
		require.Equal(t, "foo", decStrs.Row(dec.RowOffset(1)))
		require.True(t, dec.RowIsNull(2))
		require.Equal(t, int64(100), decInts.Row(dec.RowOffset(3)))

		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, 0, decInts.Rows())
		require.Equal(t, 0, decStrs.Rows())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec, _, _ := newTestVariant()
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec, _, _ := newTestVariant()
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
}

func TestColVariant_State(t *testing.T) {
	v, _, _ := newTestVariant()

	var buf Buffer
	v.EncodeState(&buf)
	require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, buf.Buf)

	t.Run("Basic", func(t *testing.T) {
		dec, _, _ := newTestVariant()
		require.NoError(t, dec.DecodeState(NewReader(bytes.NewReader(buf.Buf))))
		require.Equal(t, VariantDiscriminatorsBasic, dec.mode)
	})
	t.Run("Compact", func(t *testing.T) {
		var b Buffer
		b.PutUInt64(VariantDiscriminatorsCompact)

		dec, _, _ := newTestVariant()
		require.NoError(t, dec.DecodeState(NewReader(bytes.NewReader(b.Buf))))
		require.Equal(t, VariantDiscriminatorsCompact, dec.mode)
	})
	t.Run("Unknown", func(t *testing.T) {
		var b Buffer
		b.PutUInt64(2)

		dec, _, _ := newTestVariant()
		require.Error(t, dec.DecodeState(NewReader(bytes.NewReader(b.Buf))))
	})
	t.Run("EOF", func(t *testing.T) {
		dec, _, _ := newTestVariant()
		require.ErrorIs(t, dec.DecodeState(NewReader(bytes.NewReader(nil))), io.EOF)
	})
}

func TestColVariant_DecodeCompact(t *testing.T) {
	// Granule of 2 rows of single Int64 variant, then plain granule of
	// String and NULL rows.
	var buf Buffer
	buf.PutUVarInt(2)
	buf.PutUInt8(variantGranuleCompact)
	buf.PutUInt8(0)
	buf.PutUVarInt(2)
	buf.PutUInt8(variantGranulePlain)
	buf.PutUInt8(1)
	buf.PutUInt8(VariantNullDiscriminator)

	var data Buffer
	ColInt64{42, 100}.EncodeColumn(&data)
	var strs ColStr
	strs.Append("foo")
	strs.EncodeColumn(&data)
	buf.PutRaw(data.Buf)

	const rows = 4
	dec, decInts, decStrs := newTestVariant()
	dec.mode = VariantDiscriminatorsCompact
	require.NoError(t, dec.DecodeColumn(NewReader(bytes.NewReader(buf.Buf)), rows))

	require.Equal(t, []uint8{0, 0, 1, VariantNullDiscriminator}, []uint8(dec.Discriminators))
	require.Equal(t, int64(42), decInts.Row(dec.RowOffset(0)))
	require.Equal(t, int64(100), decInts.Row(dec.RowOffset(1)))
	require.Equal(t, "foo", decStrs.Row(dec.RowOffset(2)))
	require.True(t, dec.RowIsNull(3))

	t.Run("UnknownGranule", func(t *testing.T) {
		var b Buffer
		b.PutUVarInt(1)
		b.PutUInt8(2)

		dec, _, _ := newTestVariant()
		dec.mode = VariantDiscriminatorsCompact
		require.Error(t, dec.DecodeColumn(NewReader(bytes.NewReader(b.Buf)), 1))
	})
	t.Run("GranuleTooBig", func(t *testing.T) {
		var b Buffer
		b.PutUVarInt(2)
		b.PutUInt8(variantGranuleCompact)
		b.PutUInt8(0)

		dec, _, _ := newTestVariant()
		dec.mode = VariantDiscriminatorsCompact
		require.Error(t, dec.DecodeColumn(NewReader(bytes.NewReader(b.Buf)), 1))
	})
}

func TestColVariant_DecodeInvalidDiscriminator(t *testing.T) {
	var buf Buffer
	buf.PutUInt8(7)

	dec, _, _ := newTestVariant()
	require.Error(t, dec.DecodeColumn(NewReader(bytes.NewReader(buf.Buf)), 1))
}

func TestColVariant_Prepare(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		v, _, _ := newTestVariant()
		require.NoError(t, v.Prepare())
	})
	t.Run("NotSorted", func(t *testing.T) {
		v := NewVariant(new(ColStr), new(ColInt64))
		require.Error(t, v.Prepare())
	})
	t.Run("Duplicate", func(t *testing.T) {
		v := NewVariant(new(ColInt64), new(ColInt64))
		require.Error(t, v.Prepare())
	})
	t.Run("LowCardinality", func(t *testing.T) {
		v := NewVariant(new(ColInt64), new(ColStr).LowCardinality())
		require.Equal(t, ColumnType("Variant(Int64, LowCardinality(String))"), v.Type())
		require.NoError(t, v.Prepare())
	})
}

func TestColVariant_Infer(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		v, _, _ := newTestVariant()
		require.NoError(t, v.Infer("Variant(Int64, String)"))
	})
	t.Run("Mismatch", func(t *testing.T) {
		v, _, _ := newTestVariant()
		require.Error(t, v.Infer("Variant(Int64, String, UInt8)"))
	})
}
