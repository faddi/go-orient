package orient

func (db *Database) DatasegmentRemove(segment_name string) (bool, error) {
	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	// header
	con.beginRequest(opcodes.DATACLUSTER_REMOVE)

	con.writeString(segment_name)

	con.Flush()

	return db.datasegmentRemoveResponse(con)
}

func (db *Database) datasegmentRemoveResponse(con *orientConnection) (success bool, err error) {

	_, err = con.readResponseHead()

	if err != nil {
		return
	}

	success, err = con.readBool()

	return
}
