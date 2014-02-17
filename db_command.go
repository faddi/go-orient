package orient

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/faddi/go-orient/orecord"
)

func (db *Database) command(command command) (*CommandResult, error) {
	// (mode:byte)(class-name:string)(command-payload-length:int)(command-payload)
	// todo : maybe return chan of rawRecord here to keep it consistant
	log_debug("sync command called")

	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	con.beginRequest(opcodes.COMMAND)
	con.writeByte(CommandModes.SYNC)

	// command payload
	con.writeBytes(command.bytes(con))

	err := con.Flush()

	if err != nil {
		return nil, err
	}

	// reading response
	_, err = con.readResponseHead()

	if err != nil {
		return nil, err
	}

	status, err := con.readByte()

	if err != nil {
		return nil, err
	}

	log_debug(fmt.Sprintf("got status %d (%s)", status, string(status)))

	var result *CommandResult

	switch status {
	case 110:
		// 'n' (110), null result
		// todo : add empty result
		panic("110, no records not implemented")
	case 97:
		// 'a', serialized result

		str, err := con.readString()

		if err != nil {
			break
		}

		result = &CommandResult{serializedResult: str}

	case 108:
		// 'l', collection of records
		log_info("collection of records")
		result, err = db.readRecords(con)
	case 114:
		// 'r', single record returned
		log_info("single record")
		record, err := db.readRecord(con)

		if err != nil {
			break
		}

		s := make([]*orecord.RawRecord, 1)

		s[0] = record

		result = &CommandResult{records: s}
	default:
		return nil, errors.New(fmt.Sprintf("got unexpected status %d (%s)", status, string(status)))

	}

	db.readPrefetch(con)

	if err != nil {
		return nil, err
	}

	return result, nil

}

func (db *Database) asyncCommand(command command) (chan *orecord.RawRecord, error) {
	con := db.connectionManager.getCon()

	con.writeByte(opcodes.COMMAND)
	con.writeInt(con.sessionId)
	con.writeByte(CommandModes.ASYNC)

	// command payload
	con.writeBytes(command.bytes(con))

	err := con.Flush()

	if err != nil {
		db.connectionManager.releaseCon(con)
		return nil, err
	}

	// reading response
	_, err = con.readResponseHead()

	if err != nil {
		db.connectionManager.releaseCon(con)
		return nil, err
	}

	retChan := make(chan *orecord.RawRecord)

	go func() {
		defer close(retChan)
		for {

			status, err := con.readByte()

			if err != nil {
				db.connectionManager.releaseCon(con)
				log_error(err.Error())
				return
			}

			switch status {
			case 0:
				// 0: no records remain to be fetched
				log_info("async command done, returning results")
				db.connectionManager.releaseCon(con)
				return
			case 1:
				// 1: a record is returned as a resultset
				rec, err := db.readRecord(con)

				if err != nil {
					log_error(err.Error())
					return
				}

				retChan <- rec

			case 2:
				// todo : fix prefetch
				_, _ = db.readRecord(con)
				// 2: a record is returned as pre-fetched to be loaded in client's cache only. It's not part of the result set but the client knows that it's available for later access.
			default:
				db.connectionManager.releaseCon(con)
				log_error(fmt.Sprintf("got unexpected status %d", status))
				return
			}
		}
	}()

	return retChan, nil

}

func (db *Database) readPrefetch(con *orientConnection) error {
	if protocol_version < 17 {
		return nil
	}

	for {

		b, err := con.readByte()
		log_debug(fmt.Sprintf("prefetch read byte : %d (%s)", b, string(b)))

		if err != nil {
			return err
		}

		if b == 0 {
			log_debug("returning from prefetch")
			return nil
		}

		log_debug("reading prefetch records")
		_, err = db.readRecord(con)

		if err != nil {
			return err
		}

	}
}

func (db *Database) readRecords(con *orientConnection) (*CommandResult, error) {

	num_records, err := con.readInt()

	if err != nil {
		return nil, err
	}

	log_debug(fmt.Sprintf("records in collection : %d\n", num_records))

	s := make([]*orecord.RawRecord, num_records)

	for i := int32(0); i < num_records; i++ {

		log_debug(fmt.Sprintf("reading record %d", i))
		m, err := db.readRecord(con)

		if err != nil {
			return nil, err
		}

		s[i] = m
	}

	return &CommandResult{records: s}, nil
}

func (db *Database) readRecord(con *orientConnection) (*orecord.RawRecord, error) {
	log_debug("Start read record")

	// record head
	// -2 null record
	// -3 (-3:short)(cluster-id:short)(cluster-position:long)
	// 0 (0:short)(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)
	//record_head, err := u.ReadShort(o)

	//record_head, err := con.readShort()
	//log_info(fmt.Sprintf("record byte : %d (%s)", record_head, string(record_head)))
	// todo: this is not read just for fun, use this value
	record_head, err := con.readShort()

	if err != nil {
		return nil, err
	}

	log_debug(fmt.Sprintf("record head byte : %d (%s)", record_head, string(record_head)))

	// record type
	//record_type, err := u.ReadByte(o)
	// todo: this is not read just for fun, use this value
	record_type, err := con.readByte()

	if err != nil {
		return nil, err
	}

	log_debug(fmt.Sprintf("record type byte : %d (%s)", record_type, string(record_type)))


	clusterId, err := con.readShort()

	if err != nil {
		return nil, err
	}
	log_debug(fmt.Sprintf("record clusterId : %d", clusterId))

	clusterPosition, err := con.readLong()

	if err != nil {
		return nil, err
	}
	log_debug(fmt.Sprintf("record clusterposition : %d", clusterPosition))

	recordVersion, err := con.readInt()

	if err != nil {
		return nil, err
	}
	log_debug(fmt.Sprintf("record version : %d", recordVersion))

	data, err := con.readBytes()

	if err != nil {
		return nil, err
	}

	log_debug(fmt.Sprintf("RAW record data: %s", string(data)))
	log_debug("End read record")

	// end reading response
	return &orecord.RawRecord{data, recordVersion, &orecord.RID{clusterId, clusterPosition}, RecordTypes.DOCUMENT}, nil
}

func (db *Database) CreateCommand(commandString string) *SQLCommand {
	log_debug("New command : " + commandString)
	c := new(SQLCommand)

	// defaults
	c.ImplClassName = queryTypes.SyncQuery
	c.NonTextLimit = -1
	c.Fetchplan = ""
	c.Params = nil

	c.CommandString = commandString
	c.db = db

	return c
}

type serializer interface {
	bytes(con *orientConnection) []byte
}

type executor interface {
	ExecuteSyncQuery(params ...interface{}) (*CommandResult, error)
	ExecuteAsyncQuery(params ...interface{}) (chan *orecord.RawRecord, error)
	ExecuteCommand(params ...interface{}) (*CommandResult, error)
}

type command interface {
	serializer
	executor
}

type SQLCommand struct {
	db *Database
	command

	ImplClassName string
	CommandString string
	NonTextLimit  int32
	Fetchplan     string
	Params        []interface{}
	Language      string
}

func (c *SQLCommand) ExecuteSyncQuery(params ...interface{}) (*CommandResult, error) {
	if len(params) > 0 {
		c.Params = params
	}
	c.ImplClassName = queryTypes.SyncQuery
	return c.db.command(c)
}

func (c *SQLCommand) ExecuteAsyncQuery(params ...interface{}) (chan *orecord.RawRecord, error) {
	if len(params) > 0 {
		c.Params = params
	}
	c.ImplClassName = queryTypes.AsyncQuery
	return c.db.asyncCommand(c)
}

func (c *SQLCommand) ExecuteCommand(params ...interface{}) (result *CommandResult, err error) {
	if len(params) > 0 {
		c.Params = params
	}
	c.ImplClassName = queryTypes.Command
	c.NonTextLimit = -1
	return c.db.command(c)
}

func (c *SQLCommand) bytes(con *orientConnection) []byte {

	buf := new(bytes.Buffer)
	con.setWriteTarget(buf)
	defer con.resetWriteTarget()

	if c.ImplClassName == queryTypes.SyncQuery || c.ImplClassName == queryTypes.AsyncQuery {
		// query

		// classname
		con.writeString(c.ImplClassName)

		// text
		con.writeString(c.CommandString)

		// limit
		con.writeInt(c.NonTextLimit)

		// fetchplans
		con.writeString(c.Fetchplan)

	} else {
		// command

		// classname
		con.writeString(c.ImplClassName)

		// text
		con.writeString(c.CommandString)
	}

	// params
	if len(c.Params) > 0 {
		//println("sending params")
		// simple parametrization test
		con.writeBool(true)
		con.writeBytes(c.serializeParams())

		// compositeKeyParams parametrization test (not done)
		con.writeBool(false)
		//con.writeBool(false)
		//con.writeBool(true)

		//b := orecord.EncodeFromMap(map[string]interface{} { "compositeKeyParams" : map[string]interface{} {"one": []byte("hej hej") , "two" : 2}})

		//con.writeBytes(b)
		//_ = base64.StdEncoding.EncodeToString([]byte("hej"))
		//g += "_"
		//g := "1"
		//println(g)
		//asdf := "compositeKeyParams:{\"one\":"+g+",\"one1\":"+g+",\"one2\":"+g+",\"one3\":"+g+"}"
		//println(asdf)
		//println(string(b))
		//con.writeBytes(b)
		//con.writeString("compositeKeyParams:{one:MQ==}")
	} else {
		//println("not sending params")
		con.writeInt(0)
		//con.writeBool(false) // no simple params
		//con.writeBool(false) // no compositeKeyParams
	}

	log_debug(fmt.Sprintf("command bytes on the wire : %s", string(buf.Bytes())))

	return buf.Bytes()
}

func (c *SQLCommand) serializeParams() []byte {

	var b bytes.Buffer
	var length = len(c.Params)

	b.WriteString("params:{")
	for index, param := range c.Params {
		if index == length-1 {
			b.WriteString(fmt.Sprintf("%d:%s", index, orecord.EncodeValue(param)))
		} else {
			b.WriteString(fmt.Sprintf("%d:%s,", index, orecord.EncodeValue(param)))

		}
	}
	b.WriteString("}")
	return b.Bytes()
}

type CommandResult struct {
	records          []*orecord.RawRecord
	serializedResult string
}

func (cr *CommandResult) HasRecordsResult() bool {
	if cr.records != nil {
		return true
	}

	return false
}

func (cr *CommandResult) Records() (records []*orecord.RawRecord) {
	return cr.records
}

func (cr *CommandResult) HasSerializedResult() bool {
	return cr.serializedResult != ""
}

func (cr *CommandResult) SerializedResult() string {
	return cr.serializedResult
}
