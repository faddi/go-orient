package orient

func (db *Database) DBCountRecords() (int64, error) {
	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	// header
	con.beginRequest(opcodes.DB_COUNTRECORDS)

	con.Flush()

	return db.dBCountRecordsResponse(con)
}

func (db *Database) dBCountRecordsResponse(con *orientConnection) (count int64, err error) {

	_, err = con.readResponseHead()

	if err != nil {
		return
	}

	count, err = con.readLong()

	return
}
