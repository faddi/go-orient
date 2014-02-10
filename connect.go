package orient

//(driver-name:string)(driver-version:string)(protocol-version:short)(client-id:string)(user-name:string)(user-password:string)
func Connect(adr_str, username, password, clientId string) (*Server, error) {

	o := new(orientConnection)

	err := o.orientConnect(adr_str)

	if err != nil {
		return nil, err
	}

	err = o.writeConnect(username, password, clientId)

	if err != nil {
		o.connection.Close()
		return nil, err
	}

	sessionId, err := o.connectResponse()

	if err != nil {
		o.connection.Close()
		return nil, err
	}

	o.sessionId = sessionId

	return &Server{o}, nil
}

func (o *orientConnection) writeConnect(username, password, clientId string) error {

	o.writeByte(opcodes.CONNECT)

	// -1 session id since we don't have one yet
	o.writeInt(-1)

	o.writeString(driver_name)
	o.writeString(driver_version)
	o.writeShort(protocol_version)
	o.writeString(clientId)
	o.writeString(username)
	o.writeString(password)

	o.Flush()

	return nil
}

func (o *orientConnection) connectResponse() (int32, error) {

	// we ignore the current session id returned since we dont have one yet
	_, err := o.readResponseHead()

	if err != nil {
		return 0, err
	}

	sessId, err := o.readInt()

	if err != nil {
		return 0, err
	}

	return sessId, nil
}
