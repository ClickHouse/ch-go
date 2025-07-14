package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func main() {
	ctx := context.Background()

	// Example with SSH authentication
	client, err := ch.Dial(ctx, ch.Options{
		Address:    "localhost:9000",
		Database:   "default",
		User:       "default",
		SSHKeyFile: "/path/to/your/private_key",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("failed to close client: %v", err)
		}
	}()

	var results proto.Results

	query := ch.Query{
		Body:   "SELECT version()",
		Result: &results,
	}

	if err := client.Do(ctx, query); err != nil {
		log.Fatal(err)
	}

	if len(results) > 0 {
		if versionCol, ok := results[0].Data.(*proto.ColStr); ok && versionCol.Rows() > 0 {
			fmt.Printf("Connected successfully! ClickHouse version: %s\n", versionCol.Row(0))
		}
	}
}
