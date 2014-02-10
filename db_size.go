package orient

func (db *Database) DBSize() (int64, error) {

	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	// header
	con.beginRequest(opcodes.DB_SIZE)

	con.Flush()

	return db.dBSizeResponse(con)
}

func (db *Database) dBSizeResponse(con *orientConnection) (size int64, err error) {

	_, err = con.readResponseHead()

	if err != nil {
		return
	}

	size, err = con.readLong()

	return
}
