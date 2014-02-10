package orecord

import (
	"fmt"
	"testing"
	"time"
)

func Test_EncodeMap(t *testing.T) {

	m := make(map[string]interface{})

	m["string"] = "hej \"hej"
	m["int8"] = int8(100)
	m["int16"] = int16(1000)
	m["int32"] = int32(30000)
	m["int64"] = int64(123123123)
	m["bool"] = false
	m["float"] = float32(2.22222)
	m["double"] = float64(2.22244422323)
	data := EncodeFromMap(m)
	fmt.Printf("data : %#v\n", string(EncodeFromMap(m)))
	dm, _ := DecodeToMap(data)
	fmt.Printf("decoded data : %#v\n", dm)
}

type struct_without_exports struct {
	a, b, c, f int
}

func Test_EncodeStructNoExportedFields(t *testing.T) {
	a := struct_without_exports{}
	_, err := EncodeFromStruct(a)

	if err == nil {
		t.Fatal("A struct with no exported fields should not pass without an error")
	}
}

type struct_with_exports struct {
	A, B, c, d int
}

func Test_EncodeStruct(t *testing.T) {
	a := struct_with_exports{}
	_, err := EncodeFromStruct(a)

	if err == nil {
		t.Fatal(err.Error())
	}
}

func Test_EncodeStructPointer(t *testing.T) {
	a := struct_with_exports{}
	_, err := EncodeFromStruct(&a)

	if err != nil {
		t.Fatal(err.Error())
	}
}

func Test_EncodeStructPointerNoExportedFields(t *testing.T) {
	a := struct_without_exports{}
	_, err := EncodeFromStruct(&a)

	if err == nil {
		t.Fatal("Pointer to struct without exported fields should not return without error")
	}
}

func Test_DecodeToStructNoExportedFields(t *testing.T) {
	a := struct_without_exports{}
	err := DecodeToStruct([]byte{}, &a)

	if err == nil {
		t.Fatal("Decoding to struct without exported fields should not return without errors")
	}
}

func Test_DecodeToStruct(t *testing.T) {
	a := struct_with_exports{}
	err := DecodeToStruct([]byte{}, &a)

	if err != nil {
		t.Fatal(err.Error())
	}
}

type struct_of_types struct {
	Bool    bool
	Int8    int8
	Int16   int16
	Int32   int32
	Int64   int64
	Float32 float32
	Float64 float64
	String  string
	Time    time.Time
}

func initStructOfTypes() *struct_of_types {
	return &struct_of_types{
		true,
		12,
		123,
		12345,
		12345678,
		123.123,
		123.123123,
		"stringval",
		time.Now(),
	}
}

func Test_EncodeDecodeChain(t *testing.T) {
	a := initStructOfTypes()

	bytedata, err := EncodeFromStruct(a)

	if err != nil {
		t.Fatalf("failed encode with error : %v\n", err)
	}

	b := new(struct_of_types)

	err = DecodeToStruct(bytedata, b)

	if err != nil {
		t.Fatalf("failed decode with error : %v\n", err)
	}

	t.Logf("a : %v\nb : %v\n", a, b)

}

func Test_NewRid(t *testing.T) {

	rid := "#-1:1000000"

	r, err := NewRid(rid)

	if err != nil {
		t.Fatal("could not parse simple rid : " + rid)
	}

	t.Log(r)
}
