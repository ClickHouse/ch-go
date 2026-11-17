// Package version resolves current module version.
package version

import (
	"runtime/debug"
	"strings"
	"sync"

	"github.com/hashicorp/go-version"
)

var once struct {
	version Value
	sync.Once
}

// Value describes client module version
type Value struct {
	Major int
	Minor int
	Patch int
	Name  string
	Raw   string
}

// Extract version Value from BuildInfo.
func Extract(info *debug.BuildInfo) Value {
	var raw string
	const pkg = "github.com/ClickHouse/ch-go"
	if strings.HasPrefix(info.Main.Path, pkg) {
		raw = info.Main.Version
	}
	for _, d := range info.Deps {
		if strings.HasPrefix(d.Path, pkg) {
			raw = d.Version
			break
		}
	}
	if v, err := version.NewVersion(raw); err == nil {
		ver := Value{
			Name: v.Prerelease(), // "alpha", "beta.1"
			Raw:  raw,
		}
		if s := v.Segments(); len(s) > 2 {
			ver.Major, ver.Minor, ver.Patch = s[0], s[1], s[2]
		}
		return ver
	}
	return devVersion()
}

func devVersion() Value {
	return Value{
		Major: 0,
		Minor: 0,
		Patch: 1,
		Name:  "dev",
		Raw:   "0.0.1-dev",
	}
}

func fromBuildInfo(info *debug.BuildInfo, ok bool) Value {
	if !ok {
		return devVersion()
	}
	return Extract(info)
}

// Get optimistically gets current module version.
//
// Does not handle replace directives.
func Get() Value {
	once.Do(func() {
		info, ok := debug.ReadBuildInfo()
		once.version = fromBuildInfo(info, ok)
	})

	return once.version
}
