package proto

import "github.com/go-faster/errors"

// Exception is server-side error.
type Exception struct {
	Code    Error
	Name    string
	Message string
	Stack   string
	Nested  bool
}

// Decode exception.
func (e *Exception) Decode(r *Reader) error {
	code, err := r.Int32()
	if err != nil {
		return errors.Wrap(err, "code")
	}
	e.Code = Error(code)

	{
		s, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "name")
		}
		e.Name = s
	}
	{
		s, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "message")
		}
		e.Message = s
	}
	{
		s, err := r.Str()
		if err != nil {
			return errors.Wrap(err, "stack trace")
		}
		e.Stack = s
	}
	nested, err := r.Bool()
	if err != nil {
		return errors.Wrap(err, "nested")
	}
	e.Nested = nested

	return nil
}
