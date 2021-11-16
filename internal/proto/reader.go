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

func (r *Reader) StrAppend(buf []byte) ([]byte, error) {
	n, err := r.Int()
	if err != nil {
		return nil, errors.Wrap(err, "read length")
	}

	r.b.Ensure(n)
	defer r.b.Reset()

	if _, err := io.ReadFull(r.s, r.b.Buf); err != nil {
		return nil, errors.Wrap(err, "read str")
	}

	return append(buf, r.b.Buf...), nil
}

func (r *Reader) StrRaw() ([]byte, error) {
	return r.StrAppend(nil)
}

func (r *Reader) Str() (string, error) {
	s, err := r.StrRaw()
	if err != nil {
		return "", errors.Wrap(err, "raw")
	}

	return string(s), err
}

func (r *Reader) Int() (int, error) {
	n, err := r.Uvarint()
	if err != nil {
		return 0, errors.Wrap(err, "uvarint")
	}
	return int(n), nil
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		s: bufio.NewReader(r),
		b: &Buffer{},
	}
}
