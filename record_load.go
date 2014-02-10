package orient

import (
	"fmt"
	"github.com/faddi/go-orient/orecord"
)

func (db *Database) RecordLoad(cluster_id int16, cluster_position int64, fetch_plan string, ignore_cache bool, load_tombstones bool) (*orecord.RawRecord, error) {
	log_debug("RecordLoad")

	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	con.beginRequest(opcodes.RECORD_LOAD)

	con.writeShort(cluster_id)
	con.writeLong(cluster_position)
	con.writeString(fetch_plan)
	con.writeBool(ignore_cache)
	con.writeBool(load_tombstones)

	con.Flush()

	rec, err := db.readRecordLoad(con)

	if rec != nil {
		// if the record was found add its position to its raw representation
		rec.Rid = orecord.NewRidByExactPosition(cluster_id, cluster_position)
	}

	return rec, err
}

func (db *Database) readRecordLoad(con *orientConnection) (*orecord.RawRecord, error) {
	log_debug("readRecordLoad")

	_, err := con.readResponseHead()

	if err != nil {
		return nil, err
	}

	status, err := con.readByte()

	if status == 0 {
		return nil, err
	}

	// status 1 or 2
	record := &orecord.RawRecord{}

	record.Data, err = con.readBytes()
	record.Version, err = con.readInt()
	record.Type, err = con.readByte()

	for {
		status, err = con.readByte()
		fmt.Printf("got status : %d\n", status)
		if status == 2 {
			prefetch_record := &orecord.RawRecord{}

			prefetch_record.Data, err = con.readBytes()
			prefetch_record.Version, err = con.readInt()
			prefetch_record.Type, err = con.readByte()
			// todo, store in some cache
		} else {
			break
		}
	}

	return record, err
}
