// Package version resolves current module version.
package version

import (
	"runtime/debug"
	"strings"
	"sync"
)

var versionOnce struct {
	version string
	sync.Once
}

// Get optimistically gets current module version.
//
// Does not handle replace directives.
func Get() string {
	versionOnce.Do(func() {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		const pkg = "github.com/go-faster/ch"
		if strings.HasPrefix(info.Main.Path, pkg) {
			versionOnce.version = info.Main.Version
		}
		for _, d := range info.Deps {
			if strings.HasPrefix(d.Path, pkg) {
				versionOnce.version = d.Version
				break
			}
		}
	})

	return versionOnce.version
}
