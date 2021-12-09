package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/format"
	"os"
	"strconv"
	"strings"
	"text/template"

	"github.com/go-faster/errors"
)

type Variant struct {
	Signed bool
	Bits   int
}

func (v Variant) Byte() bool {
	return v.Bits/8 == 1 && !v.Signed
}

func (v Variant) Type() string {
	return "Column" + v.Name()
}

func (v Variant) ColumnType() string {
	return "ColumnType" + v.Name()
}

func (v Variant) Name() string {
	var b strings.Builder
	if !v.Signed {
		b.WriteString("U")
	}
	b.WriteString("Int")
	b.WriteString(strconv.Itoa(v.Bits))
	return b.String()
}

func (v Variant) BinFunc() string {
	return fmt.Sprintf("Uint%d", v.Bits)
}

func (v Variant) ElemType() string {
	var b strings.Builder
	if !v.Signed {
		b.WriteString("u")
	}
	b.WriteString("int")
	b.WriteString(strconv.Itoa(v.Bits))
	return b.String()
}

//go:embed main.tpl
var t string

func run() error {
	var (
		tpl = template.Must(template.New("main").Parse(t))
	)
	for _, bits := range []int{
		8,
		16,
		32,
		64,
	} {
		for _, signed := range []bool{true, false} {
			v := Variant{
				Signed: signed,
				Bits:   bits,
			}
			out := new(bytes.Buffer)

			if err := tpl.Execute(out, v); err != nil {
				return errors.Wrap(err, "execute")
			}

			data, err := format.Source(out.Bytes())
			if err != nil {
				return errors.Wrap(err, "format")
			}

			name := "col_" + v.ElemType() + "_gen.go"
			if err := os.WriteFile(name, data, 0o666); err != nil {
				return errors.Wrap(err, "write file")
			}
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
