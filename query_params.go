package ch

import (
	"fmt"
	"sort"

	"github.com/ClickHouse/ch-go/proto"
)

// Parameters is a helper for building Query.Parameters.
//
// Each value is formatted with fmt and enclosed in single quotes. ClickHouse
// decodes the quoted value before parsing it as the placeholder's declared
// type. Consequently, backslash escape sequences need to survive two
// ClickHouse parsing steps. For example, use `line 1\\nline 2` to receive a
// newline, or `line 1\\\\nline 2` to receive the literal characters \ and n.
//
// EXPERIMENTAL.
func Parameters(m map[string]any) []proto.Parameter {
	var out []proto.Parameter
	for k, v := range m {
		out = append(out, proto.Parameter{
			Key:   k,
			Value: fmt.Sprintf("'%v'", v),
		})
	}
	// Sorting to make output deterministic.
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})

	return out
}
