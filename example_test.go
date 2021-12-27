package ch

import (
	"fmt"
	"time"

	"github.com/go-faster/ch/proto"
)

func ExampleQuery_multipleInputColumns() {
	var (
		body      proto.ColStr
		timestamp proto.ColDateTime64
		name      proto.ColStr
		sevText   proto.ColEnum8Auto
		sevNumber proto.ColUInt8
	)
	now := time.Date(2010, 1, 1, 10, 22, 33, 345678, time.UTC)
	for i := 0; i < 10; i++ {
		body.AppendBytes([]byte("Hello"))
		timestamp = append(timestamp, proto.ToDateTime64(now, proto.PrecisionNano))
		name.Append("name")
		sevText.Values = append(sevText.Values, "INFO")
		sevNumber = append(sevNumber, 10)
	}
	input := proto.Input{
		{Name: "timestamp", Data: timestamp.Wrap(proto.PrecisionNano)},
		{Name: "severity_text", Data: &sevText},
		{Name: "severity_number", Data: sevNumber},
		{Name: "body", Data: body},
		{Name: "name", Data: name},
	}
	fmt.Println(input.Into("logs"))

	// Output:
	// INSERT INTO logs (timestamp, severity_text, severity_number, body, name) VALUES
}
