package proto

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/go-faster/errors"
)

// Reader implements ClickHouse protocol decoding from buffered reader.
type Reader struct {
	s *bufio.Reader
	b *Buffer
}

// Uvarint reads uint64 from internal reader.
func (r *Reader) Uvarint() (uint64, error) {
	n, err := binary.ReadUvarint(r.s)
	if err != nil {
		return 0, errors.Wrap(err, "read")
	}
	return n, nil
}

// StrRaw decodes string to internal buffer and returns it directly.
//
// Do not retain returned slice.
func (r *Reader) StrRaw() ([]byte, error) {
	n, err := r.Int()
	if err != nil {
		return nil, errors.Wrap(err, "read length")
	}

	r.b.Ensure(n)
	if _, err := io.ReadFull(r.s, r.b.Buf); err != nil {
		return nil, errors.Wrap(err, "read str")
	}

	return r.b.Buf, nil
}

// StrAppend decodes string and appends it to provided buf.
func (r *Reader) StrAppend(buf []byte) ([]byte, error) {
	defer r.b.Reset()

	str, err := r.StrRaw()
	if err != nil {
		return nil, errors.Wrap(err, "raw")
	}

	return append(buf, str...), nil
}

// StrBytes decodes string and allocates new byte slice with result.
func (r *Reader) StrBytes() ([]byte, error) {
	return r.StrAppend(nil)
}

// Str decodes string.
func (r *Reader) Str() (string, error) {
	s, err := r.StrBytes()
	if err != nil {
		return "", errors.Wrap(err, "bytes")
	}

	return string(s), err
}

// Int decodes uvarint as int.
func (r *Reader) Int() (int, error) {
	n, err := r.Uvarint()
	if err != nil {
		return 0, errors.Wrap(err, "uvarint")
	}
	return int(n), nil
}

// NewReader initializes new Reader from provided io.Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		s: bufio.NewReader(r),
		b: &Buffer{},
	}
}
