package ch

import (
	"fmt"
	"sort"

	"github.com/ClickHouse/ch-go/proto"
)

// Parameters is helper for building Query.Parameters.
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
