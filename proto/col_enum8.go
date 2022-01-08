package proto

import (
	"strconv"
	"strings"

	"github.com/go-faster/errors"
)

// ColEnum8Auto is inference helper for ColEnum8.
//
// You can set Values and actual enum mapping will be inferred during query
// execution.
type ColEnum8Auto struct {
	t ColumnType

	rawToStr map[Enum8]string
	strToRaw map[string]Enum8
	raw      ColEnum8

	// Values of Enum8.
	Values []string
}

// Append value to Enum8 column.
func (e *ColEnum8Auto) Append(v string) {
	e.Values = append(e.Values, v)
}

func (e *ColEnum8Auto) parse(t ColumnType) error {
	if e.rawToStr == nil {
		e.rawToStr = map[Enum8]string{}
	}
	if e.strToRaw == nil {
		e.strToRaw = map[string]Enum8{}
	}

	elements := string(t.Elem())
	for _, elem := range strings.Split(elements, ",") {
		def := strings.TrimSpace(elem)
		// 'hello' = 1
		parts := strings.SplitN(def, "=", 2)
		if len(parts) != 2 {
			return errors.Errorf("bad enum definition %q", def)
		}
		var (
			left  = strings.TrimSpace(parts[0]) // 'hello'
			right = strings.TrimSpace(parts[1]) // 1
		)
		idx, err := strconv.ParseInt(right, 10, 8)
		if err != nil {
			return errors.Errorf("bad right side of definition %q", right)
		}
		left = strings.TrimFunc(left, func(c rune) bool {
			return c == '\''
		})

		e.strToRaw[left] = Enum8(idx)
		e.rawToStr[Enum8(idx)] = left
	}

	return nil
}

func (e *ColEnum8Auto) Infer(t ColumnType) error {
	if t.Base() != ColumnTypeEnum8 {
		return errors.Errorf("invalid base %q to infer Enum8", t.Base())
	}
	if err := e.parse(t); err != nil {
		return errors.Wrap(err, "parse type")
	}
	e.t = t
	return nil
}

func (e *ColEnum8Auto) Rows() int {
	return len(e.Values)
}

func (e *ColEnum8Auto) DecodeColumn(r *Reader, rows int) error {
	if err := e.raw.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "raw")
	}
	for _, v := range e.raw {
		s, ok := e.rawToStr[v]
		if !ok {
			return errors.Errorf("unknown enum value %d", v)
		}
		e.Values = append(e.Values, s)
	}
	return nil
}

func (e *ColEnum8Auto) Reset() {
	e.raw.Reset()
	e.Values = e.Values[:0]
}

func (e *ColEnum8Auto) Prepare() error {
	if len(e.raw) != 0 {
		return errors.New("already prepared")
	}
	for _, v := range e.Values {
		raw, ok := e.strToRaw[v]
		if !ok {
			return errors.Errorf("unknown enum value %s", v)
		}
		e.raw = append(e.raw, raw)
	}
	return nil
}

func (e *ColEnum8Auto) EncodeColumn(b *Buffer) {
	e.raw.EncodeColumn(b)
}

func (e *ColEnum8Auto) Type() ColumnType {
	return e.t
}
