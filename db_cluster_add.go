package orient

func (db *Database) DataClusterAdd(clusterType, name, location, dataSegmentName string) (int16, error) {
	// (mode:byte)(class-name:string)(command-payload-length:int)(command-payload)

	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	// header

	con.writeByte(opcodes.DATACLUSTER_ADD)
	con.writeInt(con.sessionId)

	// command
	con.writeString(clusterType)
	con.writeString(name)
	con.writeString(location)
	con.writeString(dataSegmentName)

	con.Flush()

	clusterId, err := db.dataClusterAddResponse(con)

	if err == nil {
		// TODO datasegmentname (last param)
		db.Clusters[name] = Cluster{clusterId, name, clusterType, 0}
	}

	return clusterId, err
}

func (db *Database) dataClusterAddResponse(con *orientConnection) (clusterId int16, err error) {

	_, err = con.readResponseHead()

	if err != nil {
		return 0, err
	}

	clusterId, err = con.readShort()

	return
}
