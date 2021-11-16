package proto

import (
	"fmt"

	"github.com/go-faster/errors"
)

// ServerHello is answer to ClientHello and represents ServerCodeHello message.
type ServerHello struct {
	Name        string
	Major       int
	Minor       int
	Revision    int
	Timezone    string
	DisplayName string
	Patch       int
}

// Features implemented by server.
func (s ServerHello) Features() []Feature {
	var features []Feature
	for _, f := range FeatureValues() {
		if s.Has(f) {
			features = append(features, f)
		}
	}
	return features
}

// Has reports whether Feature is implemented.
func (s ServerHello) Has(f Feature) bool {
	return f.In(s.Revision)
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

	if s.Has(FeatureServerTimezone) {
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "timezone")
		}
		s.Timezone = v
	}
	if s.Has(FeatureDisplayName) {
		v, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "display name")
		}
		s.DisplayName = v
	}
	if s.Has(FeatureVersionPatch) {
		path, err := r.Int()
		if err != nil {
			return errors.Wrap(err, "patch")
		}
		s.Patch = path
	}

	return nil
}
