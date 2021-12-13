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

type Kind byte

const (
	KindInt Kind = iota
	KindFloat
	KindIP
	KindDateTime
)

type Variant struct {
	Kind   Kind
	Signed bool
	Bits   int
}

func (v Variant) IsFloat() bool {
	return v.Kind == KindFloat
}

func (v Variant) IsInt() bool {
	return v.Kind == KindInt
}

func (v Variant) SingleByte() bool {
	return v.Bits == 8
}

func (v Variant) Byte() bool {
	return v.Bits/8 == 1 && !v.Signed && v.IsInt()
}

func (v Variant) Type() string {
	return "Col" + v.Name()
}

func (v Variant) ColumnType() string {
	return "ColumnType" + v.Name()
}

func (v Variant) New() string {
	if v.Big() {
		return v.ElemType() + "FromInt"
	}
	return v.ElemType()
}

func (v Variant) Name() string {
	if v.Kind != KindInt && v.Kind != KindFloat {
		return v.ElemType()
	}
	var b strings.Builder
	if !v.Signed {
		b.WriteString("U")
	}
	switch v.Kind {
	case KindFloat:
		b.WriteString("Float")
	case KindInt:
		b.WriteString("Int")
	}
	b.WriteString(strconv.Itoa(v.Bits))
	return b.String()
}

func (v Variant) BinFunc() string {
	return fmt.Sprintf("Uint%d", v.Bits)
}

func (v Variant) BinGet() string {
	if v.IPv6() {
		return "binIPv6"
	}
	if v.Big() {
		return fmt.Sprintf("binUInt%d", v.Bits)
	}
	return "bin." + v.BinFunc()
}

func (v Variant) IsIP() bool {
	return v.Kind == KindIP
}

func (v Variant) IPv6() bool {
	return v.IsIP() && v.Bits == 128
}

func (v Variant) IPv4() bool {
	return v.IsIP() && v.Bits == 32
}

func (v Variant) BinPut() string {
	if v.IPv6() {
		return "binPutIPv6"
	}
	if v.Big() {
		return fmt.Sprintf("binPutUInt%d", v.Bits)
	}
	return "bin.Put" + v.BinFunc()
}

func (v Variant) Big() bool {
	return v.Bits > 64
}

func (v Variant) Cast() bool {
	return v.Signed || v.IPv4()
}

func (v Variant) UnsignedType() string {
	var b strings.Builder
	if v.Big() {
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
	if v.IPv4() {
		return "IPv4"
	}
	if v.IPv6() {
		return "IPv6"
	}
	if v.Kind == KindDateTime {
		if v.Bits == 64 {
			return "DateTime64"
		}
		return "DateTime"
	}
	var b strings.Builder
	var (
		unsigned = "u"
		integer  = "int"
		float    = "float"
	)
	if v.Big() {
		unsigned = "U"
		integer = "Int"
	}
	if !v.Signed {
		b.WriteString(unsigned)
	}
	if v.IsFloat() {
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
	variants := []Variant{
		{ // Float32
			Bits:   32,
			Kind:   KindFloat,
			Signed: true,
		},
		{ // Float64
			Bits:   64,
			Kind:   KindFloat,
			Signed: true,
		},
		{ // IPv4
			Bits: 32,
			Kind: KindIP,
		},
		{ // IPv6
			Bits: 128,
			Kind: KindIP,
		},
		{ // DateTIme
			Bits:   32,
			Signed: true,
			Kind:   KindDateTime,
		},
	}
	for _, bits := range []int{
		8,
		16,
		32,
		64,
		128,
	} {
		for _, signed := range []bool{true, false} {
			variants = append(variants, Variant{
				Kind:   KindInt,
				Bits:   bits,
				Signed: signed,
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
