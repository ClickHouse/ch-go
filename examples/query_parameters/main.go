package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func main() {
	ctx := context.Background()
	conn, err := ch.Dial(ctx, ch.Options{})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			panic(err)
		}
	}()

	var (
		userID             proto.ColUInt64
		escapedRaw         proto.ColStr
		escapedInterpreted proto.ColStr
		literalBackslash   proto.ColStr
	)

	// ch.Parameters quotes each value and escapes \ and ' for the Field dump.
	// Pass Escaped-format text; \n / \t are interpreted after readQuoted.
	err = conn.Do(ctx, ch.Query{
		Body: `SELECT
			{user_id:UInt64},
			{escaped_raw:String},
			{escaped_interpreted:String},
			{literal_backslash:String}`,
		Parameters: ch.Parameters(map[string]any{
			"user_id":             12345,
			"escaped_raw":         `line 1\nline 2\tend`,
			"escaped_interpreted": "line 1\\nline 2\\tend",
			"literal_backslash":   `line 1\\nline 2`,
		}),
		Result: proto.Results{
			{Data: &userID},
			{Data: &escapedRaw},
			{Data: &escapedInterpreted},
			{Data: &literalBackslash},
		},
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("userID=%d escapedRaw=%q escapedInterpreted=%q literalBackslash=%q\n",
		userID.Row(0), escapedRaw.Row(0), escapedInterpreted.Row(0), literalBackslash.Row(0))
}
