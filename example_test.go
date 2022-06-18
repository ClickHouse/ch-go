package ch

import (
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

func ExampleQuery_multipleInputColumns() {
	var (
		body      proto.ColStr
		name      proto.ColStr
		sevText   proto.ColEnum
		sevNumber proto.ColUInt8

		ts  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
		arr = new(proto.ColStr).Array() // Array(String)
		now = time.Date(2010, 1, 1, 10, 22, 33, 345678, time.UTC)
	)
	// Append 10 rows.
	for i := 0; i < 10; i++ {
		body.AppendBytes([]byte("Hello"))
		ts.Append(now)
		name.Append("name")
		sevText.Values = append(sevText.Values, "INFO")
		sevNumber = append(sevNumber, 10)
		arr.Append([]string{"foo", "bar", "baz"})
	}
	input := proto.Input{
		{Name: "ts", Data: ts},
		{Name: "severity_text", Data: &sevText},
		{Name: "severity_number", Data: sevNumber},
		{Name: "body", Data: body},
		{Name: "name", Data: name},
		{Name: "arr", Data: arr},
	}
	fmt.Println(input.Into("logs"))

	// Output:
	// INSERT INTO "logs" ("ts","severity_text","severity_number","body","name","arr") VALUES
}
