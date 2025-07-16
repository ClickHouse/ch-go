package main

import (
	"context"
	"fmt"
	"log"
	"os"

	cryptossh "golang.org/x/crypto/ssh"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func main() {
	ctx := context.Background()

	keyData, err := os.ReadFile("/path/to/your/private_key")
	if err != nil {
		log.Fatal(err)
	}

	signer, err := cryptossh.ParsePrivateKey(keyData)
	if err != nil {
		log.Fatal(err)
	}

	client, err := ch.Dial(ctx, ch.Options{
		Address:   "localhost:9000",
		Database:  "default",
		User:      "default",
		SSHSigner: signer,
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
