package orient

func (db *Database) DataclusterDatarange(clusterNumber int16) (int64, int64, error) {

	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	// header
	con.beginRequest(opcodes.DATACLUSTER_DATARANGE)

	con.writeShort(clusterNumber)

	con.Flush()

	return db.dataClusterDatarangeResponse(con)
}

func (db *Database) dataClusterDatarangeResponse(con *orientConnection) (beginning int64, end int64, err error) {

	_, err = con.readResponseHead()

	if err != nil {
		return
	}

	beginning, err = con.readLong()

	if err != nil {
		return
	}

	end, err = con.readLong()

	return
}
