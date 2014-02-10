package orient

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// writers
func (c *orientConnection) writeString(str string) error {

	//if len(str) == 0 {
	//    err := binary.Write(c.getWriteTarget(), binary.BigEndian, int32(-1))
	//    if err != nil {
	//        return err
	//    }
	//} else {

	err := binary.Write(c.getWriteTarget(), binary.BigEndian, int32(len(str)))

	if err != nil {
		return err
	}

	err = binary.Write(c.getWriteTarget(), binary.BigEndian, []byte(str))

	if err != nil {
		return err
	}
	//}
	//
	return nil
}

func (c *orientConnection) writeBytes(b []byte) error {

	if len(b) == 0 {
		err := binary.Write(c.getWriteTarget(), binary.BigEndian, int32(-1))
		if err != nil {
			return err
		}
	} else {
		err := binary.Write(c.getWriteTarget(), binary.BigEndian, int32(len(b)))

		if err != nil {
			return err
		}

		err = binary.Write(c.getWriteTarget(), binary.BigEndian, b)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *orientConnection) writeLong(n int64) error {
	err := binary.Write(c.getWriteTarget(), binary.BigEndian, n)
	if err != nil {
		return err
	}
	return nil
}

func (c *orientConnection) writeInt(n int32) error {
	err := binary.Write(c.getWriteTarget(), binary.BigEndian, n)
	if err != nil {
		return err
	}
	return nil
}

func (c *orientConnection) writeShort(n int16) error {
	err := binary.Write(c.getWriteTarget(), binary.BigEndian, n)
	if err != nil {
		return err
	}
	return nil
}

func (c *orientConnection) writeByte(b int8) error {
	err := binary.Write(c.getWriteTarget(), binary.BigEndian, b)
	if err != nil {
		return err
	}
	return nil
}

func (c *orientConnection) writeBool(b bool) error {

	var by int8 = 1

	if b != true {
		by = 0
	}

	err := binary.Write(c.getWriteTarget(), binary.BigEndian, by)

	if err != nil {
		return err
	}

	return nil
}

// read

func (c *orientConnection) readString() (string, error) {

	i, err := c.readInt()

	if err != nil {
		return "", err
	}

	if i == -1 {
		return "", nil
	}

	buf := make([]byte, i)
	err = binary.Read(c, binary.BigEndian, buf)

	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (c *orientConnection) readBytes() ([]byte, error) {

	i, err := c.readInt()

	if err != nil {
		return nil, err
	}

	if i == -1 {
		return nil, nil
	}

	buf := make([]byte, i)
	err = binary.Read(c, binary.BigEndian, buf)

	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (c *orientConnection) readLong() (int64, error) {

	var i int64
	err := binary.Read(c, binary.BigEndian, &i)

	if err != nil {
		return i, err
	}

	return i, nil
}

func (c *orientConnection) readInt() (int32, error) {
	var i int32
	err := binary.Read(c, binary.BigEndian, &i)

	if err != nil {
		return i, err
	}

	return i, nil

}

func (c *orientConnection) readShort() (int16, error) {

	var i int16
	err := binary.Read(c, binary.BigEndian, &i)

	if err != nil {
		return i, err
	}

	return i, nil

}

func (c *orientConnection) readByte() (int8, error) {

	var i int8
	err := binary.Read(c, binary.BigEndian, &i)

	return i, err
}

func (c *orientConnection) readBool() (bool, error) {

	i, err := c.readByte()

	if err != nil {
		return false, nil
	}

	if i == 0 {
		return false, nil
	}

	if i == 1 {
		return true, nil
	}

	return false, errors.New(fmt.Sprintf("%v is not a valid boolean value", i))
}

// read record header
func (c *orientConnection) readResponseHead() (int32, error) {
	log_debug("readResponseHead")

	isErr, err := c.readBool()
	log_debug(fmt.Sprintf("error in response %b", isErr))

	sessionId, err := c.readInt()
	log_debug(fmt.Sprintf("got session id : %d", sessionId))

	if err != nil {
		return -1, err
	}

	if isErr == true {
		message, err := c.readResponseError()

		if err != nil {
			return sessionId, err
		}

		return sessionId, errors.New(message)
	}

	return sessionId, nil
}

func (c *orientConnection) readResponseError() (string, error) {

	var message string

	for {

		nestedErr, err := c.readBool()

		if err != nil {
			return message, err
		}

		if nestedErr == false {
			return message, nil
		}

		exClass, err := c.readString()

		if err != nil {
			return message, err
		}

		exMessage, err := c.readString()

		if err != nil {
			return message, err
		}

		message += fmt.Sprintf("%s\n%s\n", exClass, exMessage)

	}
}

/*
0: no records remain to be fetched
1: a record is returned as a resultset
2: a record is returned as pre-fetched to be loaded in client's cache only. It's not part of the result set but the client knows that it's available for later access.
'n', means null result
'r', means single record returned
'a', serialized result
'l', collection of records
*/

/*
An entire record serialized. The format depends if a RID is passed or an entire record with its content. In case of null record then -2 as short is passed. In case of RID -3 is passes as short and then the RID:
(-3:short)(cluster-id:short)(cluster-position:long).
In case of record:
(0:short)(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)
*/
