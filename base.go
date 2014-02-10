package orient

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"
)

// logging
var dolog bool = false
var debug_log bool = false

func log_debug(s string) {
	if debug_log == true {
		fmt.Println("DEBUG ->  " + s)
	}
}

func log_info(s string) {
	if dolog == true {
		fmt.Println("INFO ->  " + s)
	}
}

func log_warn(s string) {
	if dolog == true {
		fmt.Println("WARN ->  " + s)
	}
}

func log_error(s string) {
	if dolog == true {
		fmt.Println("ERROR -> " + s)
	}
}

const (
	driver_name      = "go-orient"
	driver_version   = "0.0.1"
	protocol_version = int16(19) // todo: do not use this value as the server protocol version, use the value you read from the server instead
)

type Cluster struct {
	Id            int16
	Name          string
	Type          string
	DataSegmentId int16
}

type orientConnection struct {
	connection *net.TCPConn
	*bufio.ReadWriter
	sessionId      int32
	server_version int16
	tmpBuffer      io.Writer
}

type Server struct {
	con *orientConnection
}

func (s *Server) SessionId() int32 {
	return s.con.sessionId
}

func (s *Server) Close() error {
	if s != nil && s.con != nil {
		return s.con.Close()
	}
	return nil
}

func (o *orientConnection) orientConnect(adr_str string) error {

	addr, err := net.ResolveTCPAddr("tcp", adr_str)

	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)

	if err != nil {
		return err
	}

	o.ReadWriter = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	o.connection = conn

	srv_protocol, err := o.readShort()

	if err != nil {
		return err
	}

	o.server_version = srv_protocol

	return nil
}

func (o *orientConnection) getWriteTarget() io.Writer {
	if o.tmpBuffer != nil {
		return o.tmpBuffer
	}

	return o.connection
}

func (o *orientConnection) resetWriteTarget() {
	o.tmpBuffer = nil
}

func (o *orientConnection) setWriteTarget(w io.Writer) {
	o.tmpBuffer = w
}

func (o *orientConnection) Close() error {
	return o.connection.Close()
}

//http://stackoverflow.com/questions/12741386/how-to-know-tcp-connection-is-closed-in-golang-net-package
func (o *orientConnection) IsOpen() bool {

	o.connection.SetReadDeadline(time.Now())

	if _, err := o.connection.Read([]byte{1}); err == io.EOF {
		o.connection.Close()
		o.connection = nil
		return false
	}

	o.connection.SetReadDeadline(time.Time{})
	return true
}

// helper fn which writes opcode followed by session id on the current connection
func (o *orientConnection) beginRequest(opcode int8) {
	o.writeByte(opcode)
	o.writeInt(o.sessionId)
	log_debug(fmt.Sprintf("sent opcode %d with session id %d", opcode, o.sessionId))
}

// debugging help
func dump_bytes_forever(con *orientConnection) {
	fmt.Println("reading bytes forever : ")
	for {
		b, _ := con.readByte()
		fmt.Printf("%s", string(b))
	}
}
