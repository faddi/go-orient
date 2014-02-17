    package orecord

import (
	"bytes"
	l "container/list"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"encoding/binary"
	"encoding/base64"
)

func DecodeToStringMap(data []byte) (map[string]string, error) {
	// Much of this is taken from https://github.com/gabipetrovay/node-orientdb/blob/master/lib/orientdb/connection/parser.js
	// needs a lot of cleanup
	fmt.Printf("data : %s\n", string(data))

	res := make(map[string]string)
	var err error

	s_data := bytes.Runes(data)

	// check if we have a class
	i_at := index(s_data, '@')

	i_col := index(s_data, ':')

	// if class is set get it and remove it from the string
	var class []rune

	if i_at != -1 && (i_col == -1 || i_at < i_col) {
		class = s_data[:i_at]
		res["class"] = "\"" + string(class) + "\""
		s_data = s_data[i_at+1:]
	}

	col_index := index(s_data, ':')

	// while more colons exist
	for col_index != -1 {

		field := s_data[:col_index]
		s_data = s_data[col_index+1:]
		commaIndex := commaIndex(s_data)
		value := s_data[:commaIndex]
		//fmt.Printf("read bytes : %v (%s)\n", value, string(value))

		res[string(field)] = string(value)

		if err != nil {
			println(err.Error())
			return nil, err
		}

		if commaIndex == len(s_data) {
			s_data = s_data[commaIndex:]
		} else {
			s_data = s_data[commaIndex+1:]
		}

		col_index = index(s_data, ':')
	}

	return res, nil
}

func commaIndex(data []rune) int {
	delim_list := l.New()

	for index, ch := range data {

		if ch == ',' && delim_list.Len() == 0 {
			return index
		}

		inString := isInString(delim_list)

		if isContainerOpener(ch) && inString == false {
			delim_list.PushFront(ch)
		} else if isContainerCloser(ch) && inString == false && ch == oppositeDelimiterOf(delim_list.Front().Value.(rune)) {
			delim_list.Remove(delim_list.Front())
		} else if ch == '"' && inString && data[index-1] != '\\' {
			delim_list.Remove(delim_list.Front())
		} else if ch == '"' && inString == false {
			delim_list.PushFront(ch)
		}
	}

	return len(data)
}

func isInString(delim_list *l.List) bool {
	return delim_list.Len() > 0 && delim_list.Front().Value.(rune) == '"'
}

func isContainerOpener(ch rune) bool {

	switch ch {
	case '{', '(', '[', '<':
		return true
	}
	return false
}

func isContainerCloser(ch rune) bool {
	switch ch {
	case '}', ')', ']', '>':
		return true
	}
	return false
}

func oppositeDelimiterOf(ch rune) rune {
	switch ch {
	case '{':
		return '}'
	case '(':
		return ')'
	case '[':
		return ']'
	case '<':
		return '>'
	default:
		return '"'
	}
}

func index(r []rune, ch rune) int {
	for key := range r {
		if r[key] == ch {
			return key
		}
	}
	return -1
}

func DecodeToMap(data []byte) (map[string]interface{}, error) {

	mdata, err := DecodeToStringMap(data)

	if err != nil {
		return nil, err
	}

	outmap := make(map[string]interface{}, len(mdata))

	for key, val := range mdata {
		//fmt.Printf("key => %s, val => %s\n", key, val)
		unquotedKey, err := strconv.Unquote(key)

		if err != nil {
			// TODO: fix this, just blindly assuming its already unquoted here

			// assume its already unquoted
			unquotedKey = key
		}

		outmap[unquotedKey], err = decodeValue(val)
		//fmt.Printf("Decoded value : %s => %s\n", val, outmap[key])

		if err != nil {
			return nil, err
		}
	}

	return outmap, nil
}

func DecodeToStruct(data []byte, sPtr interface{}) error {

	// check if sPtr is a valid type
	sValue := reflect.ValueOf(sPtr)

	if sValue.Kind() != reflect.Ptr || sPtr == nil {
		return errors.New("Invalid pointer value, target must be a pointer to struct\n")
	}

	pointedToStruct := reflect.Indirect(sValue)

	if pointedToStruct.Kind() != reflect.Struct {
		return errors.New(fmt.Sprintf("Invalid pointer value, target must be a pointer to struct. Got %v\n", pointedToStruct.Type()))
	}

	mdata, err := DecodeToStringMap(data)

	if err != nil {
		return err
	}

	// todo : fix proper debug logging
	//fmt.Printf("decoding struct with data : %v\n", mdata)

	// get all exported values from the struct and populate them from the map decoded string map

	fieldCount := pointedToStruct.NumField()

	hasExportedFields := false

	for i := 0; i < fieldCount; i++ {
		f := pointedToStruct.Field(i)
		sf := pointedToStruct.Type().Field(i)

		if f.CanSet() {
			hasExportedFields = true

			if mapval, ok := mdata[strings.ToLower(sf.Name)]; ok != false {

				// set the map value
				val, err := decodeValue(mapval)

				if err != nil {
					return err
				}

				valKind := reflect.TypeOf(val).Kind()

				if valKind != f.Kind() {
					return errors.New(fmt.Sprintf("Can not convert value %v of type %v to the struct field %v with type %v", val, valKind.String(), sf.Name, f.Kind()))
				}

				switch u := val.(type) {
				case string:
					f.SetString(u)
				case bool:
					f.SetBool(u)
				case float32:
					f.SetFloat(float64(u))
				case float64:
					f.SetFloat(float64(u))
				case int:
					f.SetInt(int64(u))
				case int64:
					f.SetInt(int64(u))
				case int32:
					f.SetInt(int64(u))
				case int16:
					f.SetInt(int64(u))
				case int8:
					f.SetInt(int64(u))
				case time.Time:
					f.Set(reflect.ValueOf(u))
				}
			} else {
				// todo : fix proper debug logging
				//fmt.Printf("struct field \"%s\" was not found in returned data\n", sf.Name)
			}
		} else {
			// todo : fix proper debug logging
			//fmt.Printf("struct field \"%s\" was not settable\n", sf.Name)
		}
	}

	if hasExportedFields == false {
		return errors.New(fmt.Sprintf("No exported fields on struct with type %v\n", pointedToStruct.Type().Name()))
	}

	return nil
}

func getArrayValues(a []rune) []string {

	values := make([]string, 0)

	for {

		// get the index of the next comma
		cIndex := commaIndex(a)

		// grab the value
		val := a[:cIndex]

		values = append(values, string(val))

		// if the index is the length of the slice its the last value
		if len(a) == cIndex {
			// just return the values no need to shorten the value slice
			return values
		} else {
			// shorten the slice by excluding the current value and comma
			a = a[cIndex+1:]
		}
	}
}

func decodeValue(strvalue string) (interface{}, error) {
	//fmt.Printf("decodeValue %s\n", strvalue)

	v := []rune(strvalue)
	length := len(v)

	// empty value
	if length == 0 {
		return nil, nil
	}

	// string value
	if v[0] == '"' && v[length-1] == '"' {

		val, err := strconv.Unquote(strvalue)

		//fmt.Printf("Decoded string %s => %s\n", strvalue, val)

		if err != nil {
			return nil, err
		}

		return val, nil
	}

	// rid
	if v[0] == '#' {
		return NewRid(strvalue)
	}

	// bool
	if strvalue == "true" {
		return true, nil
	}

	if strvalue == "false" {
		return false, nil
	}

	// is it come container?
	if isContainerOpener(v[0]) {

		switch v[0] {

		case '(', '{':
			mapdata, err := DecodeToMap([]byte(strvalue[1 : len(strvalue)-1]))

			if err != nil {
				return nil, err
			}

			return mapdata, nil
		case '[', '<':
			values := getArrayValues(v[1 : len(strvalue)-1])
			arr := make([]interface{}, len(values))

			var err error = nil

			for index, val := range values {
				arr[index], err = decodeValue(val)

				if err != nil {
					return nil, err
				}
			}

			return arr, nil

		default:
			panic("can not decode " + string(v[0]))
		}
	}


	// is it a ridbag
	if v[0] == '%' && v[length-1] == ';' {
		println("ridbag!")
		return decodeLinkCollection(strvalue)
	}

	// numbers and date
	switch v[len(v)-1] {
	// byte
	case 'b':
		i, err := strconv.ParseInt(strvalue[:length-1], 10, 8)
		if err != nil {
			return nil, err
		}
		return int8(i), nil
	// short
	case 's':
		i, err := strconv.ParseInt(strvalue[:length-1], 10, 16)
		if err != nil {
			return nil, err
		}
		return int16(i), nil
	// long
	case 'l':
		i, err := strconv.ParseInt(strvalue[:length-1], 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	// float
	case 'f':
		i, err := strconv.ParseFloat(strvalue[:length-1], 32)
		if err != nil {
			return nil, err
		}
		return float32(i), nil
	// float64 (double)
	case 'd':
		i, err := strconv.ParseFloat(strvalue[:length-1], 64)
		if err != nil {
			return nil, err
		}
		return float64(i), nil
	// unix timestamp / time
	case 't', 'a':
		i, err := strconv.ParseInt(strvalue[:length-1], 10, 64)
		if err != nil {
			return nil, err
		}
		return time.Unix(i, 0), nil
	default: // int
		i, err := strconv.ParseInt(strvalue, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(i), nil
	}
}

// decodes special link sets (ridbag and ridtree)
func decodeLinkCollection(strdata string) (interface{}, error) {

	d, err := base64.StdEncoding.DecodeString(strdata[1:len(strdata) - 1])

	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(d)
	var collection_type int8

	err = binary.Read(buf, binary.BigEndian, &collection_type)

	if err != nil {
		return nil, err
	}

	if collection_type == 1 {
		return decodeRidBag(buf)
	} else {
		return decodeTree(buf)
	}
}

// decodes rid bag to slice of rid pointers
func decodeRidBag( buf *bytes.Buffer) ([]*RID, error) {

	var count int32
	err := binary.Read(buf, binary.BigEndian, &count)

	if err != nil {
		return nil, err
	}

	ridSlice := make([]*RID, count, count)

	for i := int32(0); i < count; i++ {
		var clusterId int16
		var position int64
		err = binary.Read(buf, binary.BigEndian, &clusterId)

		if err != nil {
			return nil, err;
		}

		err = binary.Read(buf, binary.BigEndian, &position)

		if err != nil {
			return nil, err;
		}

		ridSlice[i] = NewRidByExactPosition(clusterId, position)
	}

	return ridSlice, nil
}

// not implemented yet
func decodeTree( buf *bytes.Buffer) (interface{}, error) {
	fmt.Printf("decode tree not implemented! data : %v \n", buf.Bytes())
	return "asdf", nil
}
