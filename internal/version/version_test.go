package version

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtract(t *testing.T) {
	for _, tc := range []struct {
		Name   string
		Input  debug.BuildInfo
		Output Value
	}{
		{
			Name:   "Empty",
			Output: Value{Name: "dev", Raw: "0.0.1-dev"},
		},
		{
			Name: "Main",
			Input: debug.BuildInfo{
				Main: debug.Module{
					Path:    "github.com/ClickHouse/ch-go/foo/bar",
					Version: "1.5.10",
				},
			},
			Output: Value{Major: 1, Minor: 5, Patch: 10, Raw: "1.5.10"},
		},
		{
			Name: "Invalid",
			Input: debug.BuildInfo{
				Main: debug.Module{
					Path:    "github.com/ClickHouse/ch-go/foo/bar",
					Version: "bad",
				},
			},
			Output: Value{Name: "dev", Raw: "0.0.1-dev"},
		},
		{
			Name: "Dependency",
			Input: debug.BuildInfo{
				Deps: []*debug.Module{
					{
						Path:    "github.com/ClickHouse/ch-go",
						Version: "2.110.145-alpha.0",
					},
				},
			},
			Output: Value{Major: 2, Minor: 110, Patch: 145, Name: "alpha.0", Raw: "2.110.145-alpha.0"},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			require.Equal(t, tc.Output, Extract(&tc.Input))
		})
	}
}
