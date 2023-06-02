package ch

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

type MapPair[V comparable] struct {
	Keys   *proto.ColArr[string]
	Values *proto.ColArr[V]
}

func (m MapPair[V]) Row(i int) (keys []string, values []V) {
	return m.Keys.Row(i), m.Values.Row(i)
}

func (m MapPair[V]) Append(k []string, v []V) {
	m.Keys.Append(k)
	m.Values.Append(v)
}

func NewMapPair[X comparable](v proto.ColumnOf[X]) MapPair[X] {
	return MapPair[X]{
		Keys:   new(proto.ColStr).LowCardinality().Array(),
		Values: proto.NewArray(v),
	}
}

type MapPairs struct {
	Strings MapPair[string]
	Ints    MapPair[int64]
}

func NewMapPairs() MapPairs {
	return MapPairs{
		Strings: NewMapPair[string](&proto.ColStr{}),
		Ints:    NewMapPair[int64](&proto.ColInt64{}),
	}
}

// OTEL is OpenTelemetry log model.
type OTEL struct {
	Body      proto.ColStr
	Timestamp proto.ColDateTime64
	SevText   proto.ColEnum
	SevNumber proto.ColUInt8

	TraceID proto.ColFixedStr
	SpanID  proto.ColFixedStr

	// K(String):V(String) for attributes.
	Attr MapPairs
	Res  MapPairs
}

func (t *OTEL) Input() proto.Input {
	return proto.Input{
		{Name: "body", Data: t.Body},
		{Name: "timestamp", Data: t.Timestamp},
		{Name: "trace_id", Data: t.TraceID},
		{Name: "span_id", Data: t.SpanID},
		{Name: "severity_text", Data: &t.SevText},
		{Name: "severity_number", Data: t.SevNumber},

		{Name: "attr_str_keys", Data: t.Attr.Strings.Keys},
		{Name: "attr_str_values", Data: t.Attr.Strings.Values},

		{Name: "res_str_keys", Data: t.Res.Strings.Keys},
		{Name: "res_str_values", Data: t.Res.Strings.Values},
	}
}

type OTELRow struct {
	Body           []byte
	Timestamp      int64
	SeverityNumber byte
	SeverityText   string

	AttrKeys   []string
	AttrValues []string

	ResKeys   []string
	ResValues []string

	TraceID [16]byte
	SpanID  [8]byte
}

func (t *OTEL) Append(row OTELRow) {
	t.Body.AppendBytes(row.Body)
	t.Timestamp.AppendRaw(proto.DateTime64(row.Timestamp))
	t.SevNumber.Append(row.SeverityNumber)
	t.SevText.Append(row.SeverityText)

	t.TraceID.Append(row.TraceID[:])
	t.SpanID.Append(row.SpanID[:])

	t.Res.Strings.Append(row.ResKeys, row.ResValues)
	t.Attr.Strings.Append(row.AttrKeys, row.AttrValues)
}

func NewOTEL() *OTEL {
	t := &OTEL{
		TraceID: proto.ColFixedStr{Size: 16},
		SpanID:  proto.ColFixedStr{Size: 8},
		Res:     NewMapPairs(),
		Attr:    NewMapPairs(),
	}
	if err := t.SevText.Infer(`Enum8('TRACE'=1, 'DEBUG'=2, 'INFO'=3, 'WARN'=4, 'ERROR'=5, 'FATAL'=6)`); err != nil {
		panic(err)
	}

	return t
}

func BenchmarkOTEL(b *testing.B) {
	b.Run("Encode", func(b *testing.B) {
		buf := new(proto.Buffer)
		const rows = 1000
		buf.PutString("") // no temp table
		data := NewOTEL()
		for i := uint64(0); i < rows; i++ {
			data.Append(OTELRow{
				Body:           []byte("20200415T072306-0700 INFO I like donuts"),
				SeverityNumber: 9,
				SeverityText:   "INFO",
				Timestamp:      1586960586000000000,
				TraceID:        [16]byte{12, 34},
				SpanID:         [8]byte{56, 78},

				AttrKeys: []string{
					"http.status",
					"http.url",
					"my.custom.application.tag",
				},
				AttrValues: []string{
					"Internal Server Error",
					"https://example.com",
					"hello",
				},
				ResKeys: []string{
					"service.name",
					"service.version",
					"k8s.pod.uid",
				},
				ResValues: []string{
					"donut_shop",
					"2.0.0",
					"1138528c-c36e-11e9-a1a7-42010a800198",
				},
			})
		}
		block := proto.Block{
			Info:    proto.BlockInfo{BucketNum: -1},
			Columns: len(data.Input()),
			Rows:    rows,
		}
		input := data.Input()
		require.NoError(b, block.EncodeBlock(buf, proto.Version, input))

		b.ReportAllocs()
		b.ResetTimer()
		b.SetBytes(int64(len(buf.Buf)))

		for i := 0; i < b.N; i++ {
			buf.Reset()
			buf.PutString("")

			if err := block.EncodeBlock(buf, proto.Version, input); err != nil {
				b.Fatal(err)
			}
		}
	})
}
