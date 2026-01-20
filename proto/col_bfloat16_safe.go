package proto

import (
	"encoding/binary"

	"github.com/go-faster/errors"
)

var _ = binary.LittleEndian // ClickHouse uses LittleEndian

// DecodeColumn decodes BFloat16 rows from the given reader.
// BFloat16 is stored as 16-bit (2 bytes) little-endian uint16 on the wire.
func (c *ColBFloat16) DecodeColumn(r *Reader, rows int) error {
	if rows == 0 {
		return nil
	}

	const size = 2 // BFloat16 is 2 bytes
	data, err := r.ReadRaw(size * rows)
	if err != nil {
		return errors.Wrap(err, "decode-column")
	}
	v := *c
	for i := 0; i <= len(data)-size; i += size {
		v = append(v, binary.LittleEndian.Uint16(data[i:i+size]))
	}
	*c = v
	return nil
}

func (c ColBFloat16) EncodeColumn(buf *Buffer) {
	v := c
	if len(v) == 0 {
		return
	}

	const size = 2 // BFloat16 is 2 bytes
	offset := len(buf.Buf)

	// allocate enough space to fit encoded BFloat16
	buf.Buf = append(buf.Buf, make([]byte, size*len(v))...)

	for _, vv := range v {
		binary.LittleEndian.PutUint16(buf.Buf[offset:offset+size], vv)
		offset += size
	}

}

func (c ColBFloat16) WriteColumn(w *Writer) {
	w.ChainBuffer(c.EncodeColumn)

}
