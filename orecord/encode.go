package orecord

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Encodes a map[string]interface{} to []byte
func EncodeFromMap(m map[string]interface{}) []byte {
	var buffer bytes.Buffer

	for key, value := range m {
		buffer.WriteString(fmt.Sprintf("%s:%s,", key, EncodeValue(value)))
	}

	return buffer.Bytes()[:buffer.Len()-1]
}

func EncodeValue(v interface{}) string {

	switch v := v.(type) {
	case string:
		return strconv.Quote(v)
	case bool:
		return strconv.FormatBool(v)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32) + "f"
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64) + "d"
	case int64:
		return fmt.Sprintf("%dl", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%ds", v)
	case int8:
		return fmt.Sprintf("%db", v)
	case time.Time:
		return fmt.Sprintf("%da", v.Unix())
	case map[string]interface{}:
		return fmt.Sprintf("{%s}", EncodeFromMap(v))
	}

	log.Printf("Unhandled type for value %v, type %v \n", v, reflect.TypeOf(v).String())
	return ""
}

// Encodes pointer to struct to []byte
func EncodeFromStruct(s interface{}) ([]byte, error) {
	// http://play.golang.org/p/NJXaPCJbvB
	// http://blog.golang.org/2011/09/laws-of-reflection.html

	val := reflect.ValueOf(s)

	if val.Kind() != reflect.Ptr || s == nil {
		return nil, errors.New(fmt.Sprintf("Wrong input type : %v. Only ptr to struct is accepted.\n", val.Kind()))
	}

	val = val.Elem()

	if val.Kind() != reflect.Struct {
		return nil, errors.New(fmt.Sprintf("Wrong input type : %v. Only struct and ptr to struct types are accepted.\n", val.Kind()))
	}

	var buffer bytes.Buffer

	// check if a class is set
	class := val.FieldByName("Class")

	// check for zero value and append class first if it exists
	if class.IsValid() == true {
		buffer.WriteString(fmt.Sprintf("%s@", EncodeValue(class.Interface())))
	}

	sType := val.Type()
	count := sType.NumField()

	// loop all fields and encode to buffer
	for i := 0; i < count; i++ {
		f := val.Field(i)

		// make sure the field is exported and not the class field
		if f.CanSet() && f.IsValid() && sType.Field(i).Name != "Class" {
			buffer.WriteString(fmt.Sprintf("%s:%s,", strings.ToLower(sType.Field(i).Name), EncodeValue(f.Interface())))
		}
	}

	if buffer.Len() > 0 {
		return buffer.Bytes()[:buffer.Len()-1], nil
	} else {
		// Todo: struct with a single class field could pass
		return nil, errors.New("Empty struct")
	}
}
