package proto

import "testing"

func TestColArrFrom(t *testing.T) {
	var data ColStr
	arr := data.Array()
	arr.Append([]string{"foo", "bar"})
	t.Logf("%T %+v", arr.Data, arr.Data)

	_ = ArrayOf[string](new(ColStr))

	arrArr := ArrayOf[[]string](data.Array())
	arrArr.Append([][]string{
		{"foo", "bar"},
		{"baz"},
	})
	t.Log(arrArr.Type())
	_ = arrArr
}
