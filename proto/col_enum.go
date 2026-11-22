package proto

import (
	"strconv"
	"strings"

	"github.com/go-faster/errors"
)

var (
	_ Column           = (*ColEnum)(nil)
	_ ColumnOf[string] = (*ColEnum)(nil)
	_ Inferable        = (*ColEnum)(nil)
	_ Preparable       = (*ColEnum)(nil)
)

// ColEnum is inference helper for enums.
//
// You can set Values and actual enum mapping will be inferred during query
// execution.
type ColEnum struct {
	t    ColumnType
	base ColumnType

	rawToStr map[int]string
	strToRaw map[string]int
	raw8     ColEnum8
	raw16    ColEnum16

	// Values of ColEnum.
	Values []string
}

func (e *ColEnum) raw() Column {
	if e.t.Base() == ColumnTypeEnum8 {
		return &e.raw8
	}
	return &e.raw16
}

func (e ColEnum) Row(i int) string {
	return e.Values[i]
}

// Append value to Enum8 column.
func (e *ColEnum) Append(v string) {
	e.Values = append(e.Values, v)
}

func (e *ColEnum) AppendArr(vs []string) {
	e.Values = append(e.Values, vs...)
}

func (e *ColEnum) parse(t ColumnType) error {
	bitSize := 16
	if t.Base() == ColumnTypeEnum8 {
		bitSize = 8
	}

	defs, err := parseEnumDefs(t.Elem().String(), bitSize)
	if err != nil {
		return err
	}

	e.rawToStr = make(map[int]string, len(defs))
	e.strToRaw = make(map[string]int, len(defs))
	for _, elem := range defs {
		e.strToRaw[elem.name] = elem.val
		e.rawToStr[elem.val] = elem.name
	}
	return nil
}

type enumElem struct {
	name string
	val  int
}

func skipEnumSpaces(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func parseHexDigit(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, true
	default:
		return 0, false
	}
}

func decodeEnumEscape(s string, i int) (byte, int, error) {
	if i >= len(s) {
		return 0, i, errors.Errorf("unterminated escape sequence in enum definition %q", s)
	}
	ch := s[i]
	switch ch {
	case 'a':
		return '\a', i + 1, nil
	case 'b':
		return '\b', i + 1, nil
	case 'e':
		return 0x1b, i + 1, nil
	case 'f':
		return '\f', i + 1, nil
	case 'n':
		return '\n', i + 1, nil
	case 'r':
		return '\r', i + 1, nil
	case 't':
		return '\t', i + 1, nil
	case 'v':
		return '\v', i + 1, nil
	case '0':
		return 0, i + 1, nil
	case '\\', '\'':
		return ch, i + 1, nil
	case 'x', 'X':
		if i+2 < len(s) {
			hi, okHi := parseHexDigit(s[i+1])
			lo, okLo := parseHexDigit(s[i+2])
			if okHi && okLo {
				return (hi << 4) | lo, i + 3, nil
			}
		}
		return 0, i, errors.Errorf("invalid hex escape in enum definition %q", s)
	default:
		return ch, i + 1, nil
	}
}

func parseEnumName(s string, i int) (string, int, error) {
	if i >= len(s) || s[i] != '\'' {
		return "", i, errors.Errorf("bad enum definition %q", s)
	}
	i++ // consume opening quote

	var b strings.Builder
	for i < len(s) {
		ch := s[i]
		if ch == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				b.WriteByte('\'')
				i += 2
				continue
			}
			return b.String(), i + 1, nil
		}
		if ch == '\\' {
			decoded, next, decErr := decodeEnumEscape(s, i+1)
			if decErr != nil {
				return "", next, decErr
			}
			b.WriteByte(decoded)
			i = next
			continue
		}
		b.WriteByte(ch)
		i++
	}
	return "", i, errors.Errorf("unterminated enum name in %q", s)
}

func parseEnumInt(s string, i, bitSize int) (int, int, error) {
	start := i
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == start {
		return 0, i, errors.Errorf("bad right side of definition %q", s[start:])
	}
	raw := s[start:i]
	val, err := strconv.ParseInt(raw, 10, bitSize)
	if err != nil {
		return 0, i, errors.Errorf("bad right side of definition %q", raw)
	}
	return int(val), i, nil
}

func parseEnumDefs(elements string, bitSize int) ([]enumElem, error) {
	i := skipEnumSpaces(elements, 0)
	if i >= len(elements) {
		return nil, errors.Errorf("bad enum definition %q", elements)
	}

	var out []enumElem
	for {
		name, next, err := parseEnumName(elements, i)
		if err != nil {
			return nil, err
		}
		i = skipEnumSpaces(elements, next)
		if i >= len(elements) || elements[i] != '=' {
			return nil, errors.Errorf("bad enum definition %q", elements)
		}
		i = skipEnumSpaces(elements, i+1)

		val, valNext, err := parseEnumInt(elements, i, bitSize)
		if err != nil {
			return nil, err
		}
		out = append(out, enumElem{name: name, val: val})

		i = skipEnumSpaces(elements, valNext)
		if i >= len(elements) {
			break
		}
		if elements[i] != ',' {
			return nil, errors.Errorf("bad enum definition %q", elements)
		}
		i = skipEnumSpaces(elements, i+1)
		if i >= len(elements) {
			return nil, errors.Errorf("bad enum definition %q", elements)
		}
	}
	return out, nil
}

func (e *ColEnum) Infer(t ColumnType) error {
	if !strings.HasPrefix(t.Base().String(), "Enum") {
		return errors.Errorf("invalid base %q to infer enum", t.Base())
	}
	if err := e.parse(t); err != nil {
		return errors.Wrap(err, "parse type")
	}
	base := t.Base()
	switch base {
	case ColumnTypeEnum8, ColumnTypeEnum16:
		e.base = base
	default:
		return errors.Errorf("invalid base %q", base)
	}
	e.t = t
	return nil
}

func (e *ColEnum) Rows() int {
	return len(e.Values)
}

func appendEnum[E Enum8 | Enum16](c []E, mapping map[int]string, values []string) ([]string, error) {
	for _, v := range c {
		s, ok := mapping[int(v)]
		if !ok {
			return nil, errors.Errorf("unknown enum value %d", v)
		}
		values = append(values, s)
	}
	return values, nil
}

func (e *ColEnum) DecodeColumn(r *Reader, rows int) error {
	if err := e.raw().DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "raw")
	}
	var (
		err error
		v   []string
	)
	switch e.base {
	case ColumnTypeEnum8:
		v, err = appendEnum[Enum8](e.raw8, e.rawToStr, e.Values[:0])
	case ColumnTypeEnum16:
		v, err = appendEnum[Enum16](e.raw16, e.rawToStr, e.Values[:0])
	default:
		return errors.Errorf("invalid enum base %q", e.base)
	}
	if err != nil {
		return errors.Wrap(err, "map values")
	}
	e.Values = v
	return nil
}

func (e *ColEnum) Reset() {
	e.raw().Reset()
	e.Values = e.Values[:0]
}

func (e *ColEnum) Prepare() error {
	e.raw8 = e.raw8[:0]
	e.raw16 = e.raw16[:0]
	for _, v := range e.Values {
		raw, ok := e.strToRaw[v]
		if !ok {
			return errors.Errorf("unknown enum value %q", v)
		}
		switch e.base {
		case ColumnTypeEnum8:
			e.raw8.Append(Enum8(raw))
		case ColumnTypeEnum16:
			e.raw16.Append(Enum16(raw))
		default:
			return errors.Errorf("invalid base %q", e.base)
		}
	}
	return nil
}

func (e *ColEnum) EncodeColumn(b *Buffer) {
	e.raw().EncodeColumn(b)
}

func (e *ColEnum) WriteColumn(w *Writer) {
	e.raw().WriteColumn(w)
}

func (e *ColEnum) Type() ColumnType { return e.t }
