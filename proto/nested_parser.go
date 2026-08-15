package proto

import (
	"strings"
	"unicode"

	"github.com/go-faster/errors"
)

// NestedField represents a single field within a Nested type definition.
type NestedField struct {
	Name string     // Field name (e.g., "id")
	Type ColumnType // Field type (e.g., "UInt64" or "Array(String)")
}

// ParseNestedFields parses the element string inside Nested(...).
// For example, "a UInt32, b String, c Array(Int64)" returns three NestedField entries.
//
// The parser handles:
//   - Nested parentheses: "a Array(Array(Int8)), b String"
//   - Named fields with space separator: "field_name Type"
//   - Comma-separated fields at top level only
//   - Whitespace variations
func ParseNestedFields(elem string) ([]NestedField, error) {
	elem = strings.TrimSpace(elem)
	if elem == "" {
		return nil, errors.New("empty nested type definition")
	}

	var fields []NestedField
	depth := 0 // Track parenthesis depth
	start := 0 // Start of current field

	for i := 0; i < len(elem); i++ {
		switch elem[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, errors.Errorf("unbalanced parentheses at position %d", i)
			}
		case ',':
			if depth == 0 {
				// Found top-level separator
				field, err := parseNameTypePair(strings.TrimSpace(elem[start:i]))
				if err != nil {
					return nil, err
				}
				fields = append(fields, field)
				start = i + 1
			}
		}
	}

	if depth != 0 {
		return nil, errors.New("unbalanced parentheses in nested type definition")
	}

	// Don't forget the last field
	if start < len(elem) {
		field, err := parseNameTypePair(strings.TrimSpace(elem[start:]))
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	if len(fields) == 0 {
		return nil, errors.New("no fields found in nested type definition")
	}

	return fields, nil
}

// parseNameTypePair parses "name Type" or "name Type(params)" into NestedField.
// Examples:
//   - "id UInt64" -> {Name: "id", Type: "UInt64"}
//   - "values Array(String)" -> {Name: "values", Type: "Array(String)"}
//   - "data Nullable(Int32)" -> {Name: "data", Type: "Nullable(Int32)"}
func parseNameTypePair(s string) (NestedField, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return NestedField{}, errors.New("empty field definition")
	}

	// Find the first whitespace that separates name from type
	idx := strings.IndexFunc(s, unicode.IsSpace)
	if idx == -1 {
		return NestedField{}, errors.Errorf("invalid nested field definition %q: expected 'name Type' format", s)
	}

	name := s[:idx]
	typ := strings.TrimSpace(s[idx+1:])

	if name == "" {
		return NestedField{}, errors.Errorf("empty field name in %q", s)
	}
	if typ == "" {
		return NestedField{}, errors.Errorf("empty field type in %q", s)
	}

	// Validate name doesn't contain invalid characters
	if strings.ContainsAny(name, "(),") {
		return NestedField{}, errors.Errorf("invalid field name %q: contains invalid characters", name)
	}

	return NestedField{
		Name: name,
		Type: ColumnType(typ),
	}, nil
}

// ParseNestedType parses a full Nested type string like "Nested(a UInt32, b String)".
// Returns the parsed fields.
func ParseNestedType(t ColumnType) ([]NestedField, error) {
	base := t.Base()
	if base != ColumnTypeNested {
		return nil, errors.Errorf("expected Nested type, got %q", base)
	}

	elem := t.Elem()
	if elem == "" {
		return nil, errors.New("empty Nested type definition")
	}

	return ParseNestedFields(string(elem))
}
