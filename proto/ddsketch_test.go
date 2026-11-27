package proto

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// ddSketchGamma returns the gamma that ClickHouse uses for a relative accuracy.
func ddSketchGamma(accuracy float64) float64 {
	return (1 + accuracy) / (1 - accuracy)
}

// The states come from ClickHouse 26.4.1:
//
//	clickhouse local -q "SELECT hex(quantilesDDState(0.01, 0.5)(x)) FROM (SELECT arrayJoin(<values>) AS x)"
var ddSketchServerStates = []struct {
	name   string
	values string
	state  string
	sketch DDSketch
	// ClickHouse writes an empty store in the dense form with the largest
	// int as the first index. Encode writes the sparse form with no bins.
	sameBytes bool
}{
	{
		name:   "Sparse",
		values: "[-1000000., -1., 1., 1000., 1000000.]",
		state: "02FD4A815ABF52F03F0000000000000000" +
			"01040300000000000000F03FB205000000000000F03FB205000000000000F03F" +
			"03040200000000000000F03FE40A000000000000F03F" +
			"040000000000000000",
		sketch: DDSketch{
			Gamma:    ddSketchGamma(0.01),
			Positive: []DDSketchBin{{Index: 0, Count: 1}, {Index: 345, Count: 1}, {Index: 690, Count: 1}},
			Negative: []DDSketchBin{{Index: 0, Count: 1}, {Index: 690, Count: 1}},
		},
		sameBytes: true,
	},
	{
		name:   "Dense",
		values: "[-10.3, -10., 0., 0., 10., 10.1, 10.3, 10.5, 10.7]",
		state: "02FD4A815ABF52F03F0000000000000000" +
			"010C04E601020000000000000040000000000000F03F000000000000F03F000000000000F03F" +
			"030C02E60102000000000000F03F000000000000F03F" +
			"040000000000000040",
		sketch: DDSketch{
			Gamma: ddSketchGamma(0.01),
			Positive: []DDSketchBin{
				{Index: 115, Count: 2}, {Index: 116, Count: 1}, {Index: 117, Count: 1}, {Index: 118, Count: 1},
			},
			Negative:  []DDSketchBin{{Index: 115, Count: 1}, {Index: 116, Count: 1}},
			ZeroCount: 2,
		},
		sameBytes: true,
	},
	{
		name:   "EmptyStore",
		values: "[5.]",
		state: "02FD4A815ABF52F03F0000000000000000" +
			"010C01A00102000000000000F03F" +
			"030C00FEFFFFFF0F02" +
			"040000000000000000",
		sketch: DDSketch{
			Gamma:    ddSketchGamma(0.01),
			Positive: []DDSketchBin{{Index: 80, Count: 1}},
		},
	},
}

func TestDDSketch_ServerState(t *testing.T) {
	for _, tt := range ddSketchServerStates {
		t.Run(tt.name, func(t *testing.T) {
			state, err := hex.DecodeString(tt.state)
			require.NoError(t, err)

			var got DDSketch
			requireDecode(t, state, &got)
			require.Equal(t, tt.sketch, got)
			requireNoShortRead(t, state, &got)

			if tt.sameBytes {
				var b Buffer
				tt.sketch.Encode(&b)
				require.Equal(t, state, b.Buf)
			}
		})
	}
}

func TestDDSketch_Encode(t *testing.T) {
	for _, tt := range []struct {
		name   string
		sketch DDSketch
		// decoded is the sketch after a round trip, if it is not the same.
		decoded *DDSketch
		// positive is the form of the positive store.
		positive byte
	}{
		{
			name:     "Empty",
			sketch:   DDSketch{Gamma: ddSketchGamma(0.01)},
			positive: ddSketchBinsSparse,
		},
		{
			name: "Contiguous",
			sketch: DDSketch{
				Gamma:     ddSketchGamma(0.05),
				Positive:  []DDSketchBin{{Index: -2, Count: 1}, {Index: -1, Count: 4}, {Index: 0, Count: 2}, {Index: 1, Count: 7}},
				ZeroCount: 3,
			},
			positive: ddSketchBinsDense,
		},
		{
			// Four bins for one missing index: 8 bytes of zero count are
			// not more than 4 * 2 bytes of index deltas.
			name: "OneMissingIndexInFour",
			sketch: DDSketch{
				Gamma:    ddSketchGamma(0.01),
				Positive: []DDSketchBin{{Index: 10, Count: 1}, {Index: 11, Count: 2}, {Index: 13, Count: 3}, {Index: 14, Count: 4}},
			},
			positive: ddSketchBinsDense,
		},
		{
			name: "OneMissingIndexInThree",
			sketch: DDSketch{
				Gamma:    ddSketchGamma(0.01),
				Positive: []DDSketchBin{{Index: 10, Count: 1}, {Index: 12, Count: 2}, {Index: 13, Count: 3}},
			},
			positive: ddSketchBinsSparse,
		},
		{
			name: "NotAscending",
			sketch: DDSketch{
				Gamma:    ddSketchGamma(0.01),
				Positive: []DDSketchBin{{Index: 690, Count: 1}, {Index: -3, Count: 2}, {Index: 345, Count: 3}},
				Negative: []DDSketchBin{{Index: 7, Count: 1}, {Index: 7, Count: 2}},
			},
			positive: ddSketchBinsSparse,
		},
		{
			name: "ZeroCountBin",
			sketch: DDSketch{
				Gamma:    ddSketchGamma(0.01),
				Positive: []DDSketchBin{{Index: 1, Count: 1}, {Index: 2, Count: 0}, {Index: 3, Count: 5}},
			},
			decoded: &DDSketch{
				Gamma:    ddSketchGamma(0.01),
				Positive: []DDSketchBin{{Index: 1, Count: 1}, {Index: 3, Count: 5}},
			},
			positive: ddSketchBinsDense,
		},
		{
			name: "IndexLimits",
			sketch: DDSketch{
				Gamma:    ddSketchGamma(0.01),
				Positive: []DDSketchBin{{Index: -1 << 31, Count: 1}, {Index: 1<<31 - 1, Count: 2}},
				Negative: []DDSketchBin{{Index: 1<<31 - 2, Count: 1}, {Index: 1<<31 - 1, Count: 2}},
			},
			positive: ddSketchBinsSparse,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var b Buffer
			tt.sketch.Encode(&b)
			// flag, gamma, index offset, positive store flag.
			require.Equal(t, tt.positive, b.Buf[1+8+8+1])

			want := tt.sketch
			if tt.decoded != nil {
				want = *tt.decoded
			}
			var got DDSketch
			requireDecode(t, b.Buf, &got)
			require.Equal(t, want, got)
			requireNoShortRead(t, b.Buf, &got)
		})
	}
}

func TestDDSketch_DecodeError(t *testing.T) {
	const (
		mapping   = "02FD4A815ABF52F03F0000000000000000"
		empty     = "0400"
		zeroCount = "040000000000000000"
	)
	for _, tt := range []struct {
		name  string
		state string
		err   string
	}{
		{
			name:  "MappingFlag",
			state: "06FD4A815ABF52F03F0000000000000000" + "01" + empty + "03" + empty + zeroCount,
			err:   "mapping: unexpected flag 0x06 (0x02 expected)",
		},
		{
			name:  "IndexOffset",
			state: "02FD4A815ABF52F03F000000000000F03F" + "01" + empty + "03" + empty + zeroCount,
			err:   "index offset 1 is not supported",
		},
		{
			name:  "StoreOrder",
			state: mapping + "03" + empty + "01" + empty + zeroCount,
			err:   "positive: unexpected flag 0x03 (0x01 expected)",
		},
		{
			// The form with index deltas and no counts. ClickHouse does not write it.
			name:  "BinsFlag",
			state: mapping + "01" + "0800" + "03" + empty + zeroCount,
			err:   "positive: unexpected bins flag 0x08",
		},
		{
			// Two bins: the largest int32 index, then a delta of 1.
			name:  "IndexRange",
			state: mapping + "01" + "0402" + "FEFFFFFF0F000000000000F03F" + "02000000000000F03F" + "03" + empty + zeroCount,
			err:   "positive: bin index 2147483648 is out of range",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state, err := hex.DecodeString(tt.state)
			require.NoError(t, err)

			var got DDSketch
			require.EqualError(t, got.Decode(NewReader(bytes.NewReader(state))), tt.err)
		})
	}
}
