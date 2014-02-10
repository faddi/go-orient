package orient

func (db *Database) DataClusterRemove(clusterNumber int16) (int8, error) {
	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	// header
	con.beginRequest(opcodes.DATASEGMENT_REMOVE)

	// command
	con.writeShort(clusterNumber)

	con.Flush()

	return db.dataClusterRemoveResponse(con)
}

func (db *Database) dataClusterRemoveResponse(con *orientConnection) (success int8, err error) {

	_, err = con.readResponseHead()

	if err != nil {
		return 0, err
	}

	success, err = con.readByte()

	return
}
