// Package app is helper for simple cli apps.
package app

import (
	"context"
	"fmt"
	"os"

	"go.uber.org/zap"
)

func Run(run func(ctx context.Context, lg *zap.Logger) error) {
	lg, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	if err := run(context.Background(), lg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
