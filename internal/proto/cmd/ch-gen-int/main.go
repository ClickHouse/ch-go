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
var mainTemplate string

//go:embed test.tpl
var testTemplate string

func write(name string, v Variant, t *template.Template) error {
	out := new(bytes.Buffer)
	if err := t.Execute(out, v); err != nil {
		return errors.Wrap(err, "execute")
	}
	data, err := format.Source(out.Bytes())
	if err != nil {
		return errors.Wrap(err, "format")
	}
	if err := os.WriteFile(name+".go", data, 0o600); err != nil {
		return errors.Wrap(err, "write file")
	}

	return nil
}

func run() error {
	var (
		tpl     = template.Must(template.New("main").Parse(mainTemplate))
		testTpl = template.Must(template.New("main").Parse(testTemplate))
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

			base := "col_" + v.ElemType() + "_gen"
			if err := write(base, v, tpl); err != nil {
				return errors.Wrap(err, "write")
			}
			if err := write(base+"_test", v, testTpl); err != nil {
				return errors.Wrap(err, "write test")
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
