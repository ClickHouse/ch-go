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
	return v.Bits/8 == 1 && !v.Signed && !v.Float
}

func (v Variant) Type() string {
	return "Col" + v.Name()
}

func (v Variant) ColumnType() string {
	return "ColumnType" + v.Name()
}

func (v Variant) New() string {
	if v.Custom() {
		return v.ElemType() + "FromInt"
	}
	return v.ElemType()
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

func (v Variant) BinGet() string {
	if v.Custom() {
		return fmt.Sprintf("binUInt%d", v.Bits)
	}
	return "bin." + v.BinFunc()
}

func (v Variant) BinPut() string {
	if v.Custom() {
		return fmt.Sprintf("binPutUInt%d", v.Bits)
	}
	return "bin.Put" + v.BinFunc()
}

func (v Variant) Custom() bool {
	return v.Bits > 64
}

func (v Variant) UnsignedType() string {
	var b strings.Builder
	if v.Custom() {
		b.WriteString("UInt")
	} else {
		b.WriteString("uint")
	}
	b.WriteString(strconv.Itoa(v.Bits))
	return b.String()
}

func (v Variant) ElemLower() string {
	return strings.ToLower(v.ElemType())
}

func (v Variant) ElemType() string {
	var b strings.Builder
	var (
		unsigned = "u"
		integer  = "int"
		float    = "float"
	)
	if v.Custom() {
		unsigned = "U"
		integer = "Int"
	}
	if !v.Signed {
		b.WriteString(unsigned)
	}
	if v.Float {
		b.WriteString(float)
	} else {
		b.WriteString(integer)
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
		128,
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
		base := "col_" + v.ElemLower() + "_gen"
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
