package orient

func (db *Database) DBClose() {
	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	// header
	con.writeByte(opcodes.DB_CLOSE)
	con.writeInt(con.sessionId)

	con.Flush()
}
