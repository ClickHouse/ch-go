// Package gold implements golden files.
package gold

import (
	"flag"
	"os"
	"path"
	"path/filepath"
	"testing"
)

const defaultDir = "_golden"

// Update reports whether golden files update is requested.
//
// Call Init() in TestMain to propagate.
var Update bool

// Init should be called in TestMain.
func Init() {
	flag.BoolVar(&Update, "update", false, "update golden files")
}

// Path returns path to golden file.
func Path(elems ...string) string {
	return filepath.Join(
		append([]string{defaultDir}, elems...)...,
	)
}

// ReadFile reads golden file.
func ReadFile(t testing.TB, elems ...string) []byte {
	t.Helper()

	p := Path(elems...)
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("golden file %s: %+v", path.Join(elems...), err)
	}

	return data
}
