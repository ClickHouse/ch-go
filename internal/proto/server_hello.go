package proto

import (
	"fmt"

	"github.com/go-faster/errors"
)

// ServerHello is answer to ClientHello and represents ServerCodeHello message.
type ServerHello struct {
	Name     string
	Major    int
	Minor    int
	Revision int
}

func (s ServerHello) String() string {
	return fmt.Sprintf("%s %d.%d.%d", s.Name, s.Major, s.Minor, s.Revision)
}

// Decode decodes ServerHello message from Reader.
func (s *ServerHello) Decode(r *Reader) error {
	name, err := r.Str()
	if err != nil {
		return errors.Wrap(err, "str")
	}
	s.Name = name

	major, err := r.Int()
	if err != nil {
		return errors.Wrap(err, "major")
	}
	minor, err := r.Int()
	if err != nil {
		return errors.Wrap(err, "minor")
	}
	revision, err := r.Int()
	if err != nil {
		return errors.Wrap(err, "revision")
	}

	s.Major, s.Minor, s.Revision = major, minor, revision
	return nil
}
