package proto

// Error on server side.
type Error int

func (e Error) Error() string {
	return e.String()
}

//go:generate go run github.com/dmarkham/enumer -transform snake_upper -type Error -trimprefix Err -output error_gen.go
