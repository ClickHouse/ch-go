{{- /*gotype: github.com/go-faster/ch/proto/cmd/ch-gen-col.Variant*/ -}}
// Code generated by ./cmd/ch-gen-int, DO NOT EDIT.

package proto

import (
	"encoding/binary"
{{- if .IsFloat }}
   "math"
{{- end }}
  "github.com/go-faster/errors"
)

// ClickHouse uses LittleEndian.
var _ = binary.LittleEndian

// {{ .Type }} represents {{ .Name }} column.
type {{ .Type }} []{{ .ElemType }}

// Compile-time assertions for {{ .Type }}.
var (
  _ ColInput  = {{ .Type }}{}
  _ ColResult = (*{{ .Type }})(nil)
  _ Column    = (*{{ .Type }})(nil)
)

// Type returns ColumnType of {{ .Name }}.
func ({{ .Type }}) Type() ColumnType {
  return {{ .ColumnType }}
}

// Rows returns count of rows in column.
func (c {{ .Type }}) Rows() int {
  return len(c)
}

// Reset resets data in row, preserving capacity for efficiency.
func (c *{{ .Type }}) Reset() {
  *c = (*c)[:0]
}

// NewArr{{ .Name }} returns new Array({{ .Name }}).
func NewArr{{ .Name }}() *ColArr {
  return &ColArr{
    Data: new({{ .Type }}),
  }
}

// Append{{ .Name }} appends slice of {{ .ElemType }} to Array({{ .Name }}).
func (c *ColArr) Append{{ .Name }}(data []{{ .ElemType }}) {
  d := c.Data.(*{{ .Type }})
  *d = append(*d, data...)
  c.Offsets = append(c.Offsets, uint64(len(*d)))
}

// EncodeColumn encodes {{ .Name }} rows to *Buffer.
func (c {{ .Type }}) EncodeColumn(b *Buffer) {
  {{- if .Byte }}
  b.Buf = append(b.Buf, c...)
  {{- else if .SingleByte }}
  start := len(b.Buf)
  b.Buf = append(b.Buf, make([]byte, len(c))...)
  for i := range c {
    b.Buf[i + start] = {{ .UnsignedType }}(c[i])
  }
  {{- else }}
  const size = {{ .Bits }} / 8
  offset := len(b.Buf)
  b.Buf = append(b.Buf, make([]byte, size * len(c))...)
  for _, v := range c {
    {{ .BinPut }}(
      b.Buf[offset:offset+size],
    {{- if .IsFloat }}
      math.{{ .Name }}bits(v),
    {{- else if .Cast }}
      {{ .UnsignedType }}(v),
    {{- else }}
      v,
    {{- end }}
    )
    offset += size
  }
  {{- end }}
}

// DecodeColumn decodes {{ .Name }} rows from *Reader.
func (c *{{ .Type }}) DecodeColumn(r *Reader, rows int) error {
  if rows == 0 {
    return nil
  }
  {{- if .SingleByte }}
  data, err := r.ReadRaw(rows)
  {{- else }}
  const size = {{ .Bits }} / 8
  data, err := r.ReadRaw(rows * size)
  {{- end }}
  if err != nil {
    return errors.Wrap(err, "read")
  }
  {{- if .Byte }}
  *c = append(*c, data...)
  {{- else if .SingleByte }}
  v := *c
  v = append(v, make([]{{ .ElemType }}, rows)...)
  for i := range data {
    v[i] = {{ .ElemType }}(data[i])
  }
  *c = v
  {{- else }}
  v := *c
  // Move bound check out of loop.
  //
  // See https://github.com/golang/go/issues/30945.
  _ = data[len(data)-size]
  for i := 0; i <= len(data)-size; i += size {
    v = append(v,
    {{- if .IsFloat }}
      math.{{ .Name }}frombits(bin.{{ .BinFunc }}(data[i:i+size])),
    {{- else if .Cast }}
     {{ .ElemType }}({{ .BinGet }}(data[i:i+size])),
    {{- else }}
      {{ .BinGet }}(data[i:i+size]),
    {{- end }}
    )
  }
  *c = v
  {{- end }}
  return nil
}
