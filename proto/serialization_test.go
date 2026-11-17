package proto

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type customTestColumn struct {
	ColUInt64
	prepared bool
	failure  error
}

func (c *customTestColumn) Prepare() error {
	c.prepared = true
	return nil
}

func (c *customTestColumn) EncodeSerialization(b *Buffer, _ int) error {
	if !c.prepared {
		return errors.New("metadata written before Prepare")
	}
	if c.failure != nil {
		return c.failure
	}
	// Explicit default-kind metadata exercises the hook with an ordinary payload.
	b.PutByte(0)
	return nil
}

func TestCustomSerializationBlock(t *testing.T) {
	for _, rows := range []int{0, 2} {
		column := &customTestColumn{ColUInt64: ColUInt64{7, 9}[:rows]}
		input := Input{{Name: "custom", Data: column}, {Name: "ordinary", Data: ColUInt64{11, 13}[:rows]}}
		block := Block{Columns: 2, Rows: rows}
		var expected Buffer
		block.EncodeAware(&expected, Version)
		expected.PutString("custom")
		expected.PutString("UInt64")
		expected.PutBool(true)
		expected.PutByte(0)
		for _, value := range column.ColUInt64 {
			expected.PutUInt64(value)
		}
		expected.PutString("ordinary")
		expected.PutString("UInt64")
		expected.PutBool(false)
		for _, value := range (ColUInt64{11, 13})[:rows] {
			expected.PutUInt64(value)
		}

		var encoded Buffer
		require.NoError(t, block.EncodeBlock(&encoded, Version, input))
		require.Equal(t, expected.Buf, encoded.Buf)
		for _, streaming := range []bool{false, true} {
			column.prepared = false
			var output bytes.Buffer
			var scratch Buffer
			writer := NewWriter(&output, &scratch)
			if streaming {
				writer = NewStreamingWriter(&output, &scratch)
			}
			require.NoError(t, block.WriteBlock(writer, Version, input))
			_, err := writer.Flush()
			require.NoError(t, err)
			require.Equal(t, expected.Buf, output.Bytes())
		}
	}
}

func TestCustomSerializationErrors(t *testing.T) {
	failure := errors.New("unsupported serialization kind")
	for _, version := range []int{0, FeatureCustomSerialization.Version() - 1, Version} {
		column := &customTestColumn{ColUInt64: ColUInt64{1}, failure: failure}
		input := Input{{Name: "custom", Data: column}}
		block := Block{Columns: 1, Rows: 1}
		var encoded Buffer
		check := func(err error) {
			t.Helper()
			if version < FeatureCustomSerialization.Version() {
				require.ErrorContains(t, err, "requires custom serialization")
			} else {
				require.ErrorIs(t, err, failure)
			}
		}
		check(block.EncodeBlock(&encoded, version, input))
		for _, streaming := range []bool{false, true} {
			var output bytes.Buffer
			var scratch Buffer
			writer := NewWriter(&output, &scratch)
			if streaming {
				writer = NewStreamingWriter(&output, &scratch)
			}
			check(block.WriteBlock(writer, version, input))
		}
	}
}
