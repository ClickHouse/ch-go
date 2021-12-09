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
	Float  bool
	Signed bool
	Bits   int
}

func (v Variant) SingleByte() bool {
	return v.Bits == 8
}

func (v Variant) Byte() bool {
	return v.Bits/8 == 1 && !v.Signed
}

func (v Variant) Type() string {
	return "Col" + v.Name()
}

func (v Variant) ColumnType() string {
	return "ColumnType" + v.Name()
}

func (v Variant) Name() string {
	var b strings.Builder
	if !v.Signed {
		b.WriteString("U")
	}
	if v.Float {
		b.WriteString("Float")
	} else {
		b.WriteString("Int")
	}
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
	if v.Float {
		b.WriteString("float")
	} else {
		b.WriteString("int")
	}
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
	var variants []Variant
	for _, bits := range []int{
		8,
		16,
		32,
		64,
	} {
		for _, signed := range []bool{true, false} {
			variants = append(variants, Variant{
				Bits:   bits,
				Signed: signed,
			})
		}
		switch bits {
		case 32, 64:
			variants = append(variants, Variant{
				Bits:   bits,
				Float:  true,
				Signed: true,
			})
		}
	}
	for _, v := range variants {
		base := "col_" + v.ElemType() + "_gen"
		if err := write(base, v, tpl); err != nil {
			return errors.Wrap(err, "write")
		}
		if err := write(base+"_test", v, testTpl); err != nil {
			return errors.Wrap(err, "write test")
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
