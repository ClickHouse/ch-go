package main

import (
	"fmt"

	"github.com/go-faster/ch/internal/version"
)

func main() {
	fmt.Println("version", version.Get())
}
