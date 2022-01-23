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

type Value struct {
	Major int
	Minor int
	Patch int
	Name  string
}

// Get optimistically gets current module version.
//
// Does not handle replace directives.
func Get() Value {
	once.Do(func() {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		var (
			raw string
			ver Value
		)
		const pkg = "github.com/go-faster/ch"
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
			ver = Value{
				Name: v.Prerelease(), // "alpha", "beta.1"
			}
			if s := v.Segments(); len(s) > 2 {
				ver.Major, ver.Minor, ver.Patch = s[0], s[1], s[2]
			}
		} else {
			ver = Value{
				// Zero-versioned dev version.
				Name: "dev",
			}
		}
		once.version = ver
	})

	return once.version
}
