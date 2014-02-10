package orient

func (db *Database) DatasegmentAdd(segment_name, location string) (int32, error) {
	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	// header
	con.beginRequest(opcodes.DATASEGMENT_ADD)

	con.writeString(segment_name)
	con.writeString(location)

	con.Flush()

	return db.dataSegmentAddResponse(con)
}

func (db *Database) dataSegmentAddResponse(con *orientConnection) (dataSegmentId int32, err error) {

	_, err = con.readResponseHead()

	if err != nil {
		return
	}

	dataSegmentId, err = con.readInt()

	return
}
