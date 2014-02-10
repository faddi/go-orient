package orient

import (
	"fmt"
)

func (db *Database) RecordDelete(cluster_id int16, cluster_position int64, record_version int32, async bool, callback func(response bool, err error)) {

	con := db.connectionManager.getCon()

	con.beginRequest(opcodes.RECORD_DELETE)
	log_debug(fmt.Sprintf("deleting record %d:%d", cluster_id, cluster_position))

	con.writeShort(cluster_id)
	con.writeLong(cluster_position)
	con.writeInt(record_version)
	con.writeBool(async)

	if async {
		go func() {
			response, err := db.readRecordDelete(con)
			db.connectionManager.releaseCon(con)

			callback(response, err)
		}()
	} else {
		response, err := db.readRecordDelete(con)
		db.connectionManager.releaseCon(con)
		callback(response, err)
	}
}

func (db *Database) readRecordDelete(con *orientConnection) (bool, error) {

	_, err := con.readResponseHead()

	if err != nil {
		return false, err
	}

	return con.readBool()
}
