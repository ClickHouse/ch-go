package main

import (
	"fmt"

	"github.com/ClickHouse/ch-go/internal/version"
)

func main() {
	fmt.Println("version", version.Get())
}
