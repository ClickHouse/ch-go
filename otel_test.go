package ch

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/proto"
)

// OTEL is OpenTelemetry log model.
type OTEL struct {
	Body      proto.ColStr
	Timestamp proto.ColDateTime64
	SevText   proto.ColEnum8Auto
	SevNumber proto.ColUInt8

	TraceID proto.ColFixedStr
	SpanID  proto.ColFixedStr

	// K(String):V(String) for attributes.
	AttrStringValues    proto.ColStr
	AttrStringValuesArr proto.ColArr
	AttrStringKeys      proto.ColStr
	AttrStringKeysArr   proto.ColArr

	// K(String):V(String) for resource.
	ResStringValues    proto.ColStr
	ResStringValuesArr proto.ColArr
	ResStringKeys      proto.ColStr
	ResStringKeysArr   proto.ColArr
}

func (t *OTEL) Input() proto.Input {
	return proto.Input{
		{Name: "body", Data: t.Body},
		{Name: "timestamp", Data: t.Timestamp.Wrap(proto.PrecisionNano)},
		{Name: "severity_text", Data: &t.SevText},
		{Name: "severity_number", Data: t.SevNumber},

		{Name: "attr_str_keys", Data: t.AttrStringKeysArr},
		{Name: "attr_str_values", Data: t.AttrStringValuesArr},

		{Name: "res_str_keys", Data: t.ResStringKeysArr},
		{Name: "res_str_values", Data: t.ResStringValuesArr},
	}
}

func (t *OTEL) appendStrArr(values *proto.ColStr, arr *proto.ColArr, data []string) {
	values.ArrAppend(arr, data)
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
	t.Timestamp.Append(proto.DateTime64(row.Timestamp))
	t.SevNumber.Append(row.SeverityNumber)
	t.SevText.Append(row.SeverityText)

	t.ResStringKeys.ArrAppend(&t.ResStringKeysArr, row.ResKeys)
	t.ResStringValues.ArrAppend(&t.ResStringValuesArr, row.ResValues)

	t.AttrStringKeys.ArrAppend(&t.AttrStringKeysArr, row.AttrKeys)
	t.AttrStringValues.ArrAppend(&t.AttrStringValuesArr, row.AttrValues)
}

func NewOTEL() *OTEL {
	t := &OTEL{}

	// Bind arrays.
	t.AttrStringKeysArr.Data = &t.AttrStringKeys
	t.AttrStringValuesArr.Data = &t.AttrStringValues

	t.ResStringValuesArr.Data = &t.ResStringValues
	t.ResStringKeysArr.Data = &t.ResStringKeys

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
