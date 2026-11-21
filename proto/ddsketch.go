package proto

import (
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"
)

// DDSketch is the state of the `quantileDD` and `quantilesDD` aggregate
// functions. Use it with [ColAggregateFunction].
//
// The state does not depend on the parameters or on the argument type of
// the function. A function with a `Nullable` argument is different: it
// writes one more byte before the sketch, and DDSketch does not read it.
//
// See https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/quantileddsketch
// and https://www.vldb.org/pvldb/vol12/p2195-masson.pdf.
type DDSketch struct {
	// Gamma is the base of the logarithmic mapping from a value to a bin
	// index. ClickHouse uses (1 + a) / (1 - a) for the relative accuracy a,
	// and accepts only a finite value larger than 1.
	Gamma float64
	// Positive are the bins of the values larger than zero.
	Positive []DDSketchBin
	// Negative are the bins of the values smaller than zero. The bin index
	// comes from the absolute value.
	Negative []DDSketchBin
	// ZeroCount is the count of the values that are zero.
	ZeroCount float64
}

// DDSketchBin is the count of the values that map to one bin index.
type DDSketchBin struct {
	// Index is trunc(log(v) / log(Gamma)) for the value v.
	Index int32
	// Count must be finite and not negative.
	Count float64
}

// Each block of the state starts with a flag byte.
// See src/AggregateFunctions/DDSketch/DDSketchEncoding.h in ClickHouse.
const (
	ddSketchFlagMapping   byte = 0x02 // logarithmic mapping: gamma, index offset
	ddSketchFlagPositive  byte = 0x01
	ddSketchFlagNegative  byte = 0x03
	ddSketchFlagZeroCount byte = 0x04

	// Bins as (index delta, count) pairs.
	ddSketchBinsSparse byte = 0x04
	// Bins as the first index, the index step, and one count per index.
	ddSketchBinsDense byte = 0x0c
)

// ddSketchMaxBins is the largest number of bins that ClickHouse reads for
// one store (max_bins_deserialize). The bins count comes from the wire, so
// Decode does not allocate for more bins than this before it reads them.
const ddSketchMaxBins = 65536

// Encode encodes DDSketch to *Buffer.
func (s DDSketch) Encode(b *Buffer) {
	b.PutByte(ddSketchFlagMapping)
	b.PutFloat64(s.Gamma)
	b.PutFloat64(0) // ClickHouse rejects an index offset that is not zero.
	b.PutByte(ddSketchFlagPositive)
	encodeDDSketchBins(b, s.Positive)
	b.PutByte(ddSketchFlagNegative)
	encodeDDSketchBins(b, s.Negative)
	b.PutByte(ddSketchFlagZeroCount)
	b.PutFloat64(s.ZeroCount)
}

// Decode decodes DDSketch from *Reader.
//
// The bins come back without the bins that have a zero count.
func (s *DDSketch) Decode(r *Reader) error {
	if err := expectDDSketchFlag(r, ddSketchFlagMapping); err != nil {
		return errors.Wrap(err, "mapping")
	}
	gamma, err := r.Float64()
	if err != nil {
		return errors.Wrap(err, "gamma")
	}
	offset, err := r.Float64()
	if err != nil {
		return errors.Wrap(err, "index offset")
	}
	if offset != 0 {
		return errors.Errorf("index offset %v is not supported", offset)
	}
	if err := expectDDSketchFlag(r, ddSketchFlagPositive); err != nil {
		return errors.Wrap(err, "positive")
	}
	positive, err := decodeDDSketchBins(r)
	if err != nil {
		return errors.Wrap(err, "positive")
	}
	if err := expectDDSketchFlag(r, ddSketchFlagNegative); err != nil {
		return errors.Wrap(err, "negative")
	}
	negative, err := decodeDDSketchBins(r)
	if err != nil {
		return errors.Wrap(err, "negative")
	}
	if err := expectDDSketchFlag(r, ddSketchFlagZeroCount); err != nil {
		return errors.Wrap(err, "zero count")
	}
	zeroCount, err := r.Float64()
	if err != nil {
		return errors.Wrap(err, "zero count")
	}
	*s = DDSketch{
		Gamma:     gamma,
		Positive:  positive,
		Negative:  negative,
		ZeroCount: zeroCount,
	}
	return nil
}

func expectDDSketchFlag(r *Reader, flag byte) error {
	got, err := r.Byte()
	if err != nil {
		return errors.Wrap(err, "flag")
	}
	if got != flag {
		return errors.Errorf("unexpected flag 0x%02x (0x%02x expected)", got, flag)
	}
	return nil
}

// encodeDDSketchBins writes the bins of one store.
//
// The dense form writes one count for each index from the first bin to the
// last bin, so it needs bins in ascending index order with no index twice.
// The sparse form is correct for bins in any order.
func encodeDDSketchBins(b *Buffer, bins []DDSketchBin) {
	ascending := len(bins) > 0
	for i := 1; i < len(bins); i++ {
		if bins[i].Index <= bins[i-1].Index {
			ascending = false
			break
		}
	}
	if ascending {
		first := int64(bins[0].Index)
		last := int64(bins[len(bins)-1].Index)
		missing := last - first + 1 - int64(len(bins))
		// The same rule as ClickHouse: a missing index costs one float64
		// (8 bytes) in the dense form, and ClickHouse counts 2 bytes for
		// the index delta of each bin in the sparse form.
		if missing*8 <= int64(len(bins))*2 {
			b.PutByte(ddSketchBinsDense)
			b.PutUVarInt(uint64(last - first + 1))
			b.Buf = binary.AppendVarint(b.Buf, first)
			b.Buf = binary.AppendVarint(b.Buf, 1)
			next := 0
			for index := first; index <= last; index++ {
				if int64(bins[next].Index) != index {
					b.PutFloat64(0)
					continue
				}
				b.PutFloat64(bins[next].Count)
				next++
			}
			return
		}
	}
	b.PutByte(ddSketchBinsSparse)
	b.PutUVarInt(uint64(len(bins)))
	prev := int64(0)
	for _, bin := range bins {
		b.Buf = binary.AppendVarint(b.Buf, int64(bin.Index)-prev)
		b.PutFloat64(bin.Count)
		prev = int64(bin.Index)
	}
}

// decodeDDSketchBins reads the bins of one store.
func decodeDDSketchBins(r *Reader) ([]DDSketchBin, error) {
	form, err := r.Byte()
	if err != nil {
		return nil, errors.Wrap(err, "flag")
	}
	if form != ddSketchBinsDense && form != ddSketchBinsSparse {
		return nil, errors.Errorf("unexpected bins flag 0x%02x", form)
	}
	n, err := r.UVarInt()
	if err != nil {
		return nil, errors.Wrap(err, "bins count")
	}
	var index, step int64
	if form == ddSketchBinsDense {
		if index, err = binary.ReadVarint(r); err != nil {
			return nil, errors.Wrap(err, "first index")
		}
		if step, err = binary.ReadVarint(r); err != nil {
			return nil, errors.Wrap(err, "index step")
		}
	}
	if n == 0 {
		return nil, nil
	}

	bins := make([]DDSketchBin, 0, min(n, ddSketchMaxBins))
	for i := uint64(0); i < n; i++ {
		if form == ddSketchBinsSparse {
			if step, err = binary.ReadVarint(r); err != nil {
				return nil, errors.Wrap(err, "index delta")
			}
		}
		// The sparse form has a delta before each bin. The dense form has
		// one step between two bins, so the first bin does not move.
		if form == ddSketchBinsSparse || i > 0 {
			index += step
		}
		count, err := r.Float64()
		if err != nil {
			return nil, errors.Wrap(err, "count")
		}
		if count == 0 {
			// The dense form has a zero count for each missing index.
			continue
		}
		if index < math.MinInt32 || index > math.MaxInt32 {
			return nil, errors.Errorf("bin index %d is out of range", index)
		}
		bins = append(bins, DDSketchBin{Index: int32(index), Count: count})
	}
	return bins, nil
}
