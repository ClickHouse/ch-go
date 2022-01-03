//go:build (amd64 || arm64) && !purego

package proto

import (
	"unsafe"

	"github.com/go-faster/errors"
	"github.com/google/uuid"
)

func (c *ColUUID) DecodeColumn(r *Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	*c = append(*c, make([]uuid.UUID, rows)...)

	// Memory layout of [N]UUID is same as [N*sizeof(UUID)]byte.
	// So just interpret c as byte slice and read data into it.
	s := *(*slice)(unsafe.Pointer(c)) // #nosec: G103 // memory layout matches
	s.Len *= 16
	s.Cap *= 16
	dst := *(*[]byte)(unsafe.Pointer(&s)) // #nosec: G103 // memory layout matches
	if err := r.ReadFull(dst); err != nil {
		return errors.Wrap(err, "read full")
	}

	return nil
}
