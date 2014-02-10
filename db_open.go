package orient

func DBOpen(adr_str, db_type, db_name, username, password, clientId string) (*Database, error) {

	db := &Database{}
	var err error
	err = db.initConnectionManager(1, adr_str, db_type, db_name, username, password, clientId)

	if err != nil {
		return nil, err
	}

	return db, err
}

//(driver-name:string)(driver-version:string)(protocol-version:short)(client-id:string)(database-name:string)(database-type:string)(user-name:string)(user-password:string)
func (o *orientConnection) dBOpen(db_type, db_name, username, password, clientId string) error {

	o.writeByte(opcodes.DB_OPEN)
	o.writeInt(-1)
	o.writeString(driver_name)
	o.writeString(driver_version)
	o.writeShort(protocol_version)
	o.writeString(clientId)
	o.writeString(db_name)
	o.writeString(db_type)
	o.writeString(username)
	o.writeString(password)

	o.Flush()

	return nil
}

//Response: (session-id:int)(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)(cluster-type:string)(cluster-dataSegmentId:short)](cluster-config:bytes)(orientdb-release:string)
func (o *orientConnection) DBOpenResponse() (map[string]Cluster, error) {

	// ignore saving session id it should always be -1 here
	_, err := o.readResponseHead()

	if err != nil {
		return nil, err
	}

	sessId, err := o.readInt()

	if err != nil {
		return nil, err
	}

	o.sessionId = sessId

	clusters, err := readClusterInfo(o)

	if err != nil {
		return nil, err
	}

	// clusterconfig
	// TODO: handle
	_, err = o.readBytes()

	if err != nil {
		return nil, err
	}

	// release
	if o.server_version > 13 {
		// TODO: handle
		_, err = o.readString()

		if err != nil {
			return nil, err
		}
	}

	return clusters, nil
}
