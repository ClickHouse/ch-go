//go:build go1.18

package proto

import "testing"

func TestColArrFrom(t *testing.T) {
	var data ColStr
	arr := data.Array()
	arr.Append([]string{"foo", "bar"})
	_ = arr.Data.Buf
	t.Logf("%T %+v", arr.Data, arr.Data)

	_ = ArrayOf[string](&ColStr{})
	_ = ArrayOf[[]string](ArrayOf[string](&ColStr{}))
}
