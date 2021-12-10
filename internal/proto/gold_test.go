package proto

import (
	"reflect"
	"testing"

	"github.com/go-faster/ch/internal/gold"
)

func typeName(v interface{}) string {
	if t := reflect.TypeOf(v); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

// Gold checks golden version of v encoding.
func Gold(t testing.TB, v AwareEncoder, name ...string) {
	t.Helper()
	if len(name) == 0 {
		name = []string{"type", typeName(v)}
	}
	var b Buffer
	v.EncodeAware(&b, Version)
	gold.Bytes(t, b.Buf, name...)
}
