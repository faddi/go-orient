package orient

import (
	"errors"
	"github.com/faddi/go-orient/orecord"
)

func (db *Database) RecordCreate(datasegmentId int32, clusterId int16, record_content []byte, record_type int8, mode int8) (clusterPosition int64, recordVersion int32, err error) {
	log_debug("RecordCreate")
	/*
		record-type is:
		'b': raw bytes
		'f': flat data
		'd': document
	*/
	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	con.beginRequest(opcodes.RECORD_CREATE)

	con.writeInt(datasegmentId)
	con.writeShort(clusterId)
	con.writeBytes(record_content)
	con.writeByte(record_type)
	con.writeByte(mode)

	con.Flush()

	return db.recordCreateResponse(con)
}

func (db *Database) recordCreateResponse(con *orientConnection) (clusterPosition int64, recordVersion int32, err error) {
	log_debug("recordCreateResponse")

	_, err = con.readResponseHead()

	if err != nil {
		return
	}

	clusterPosition, err = con.readLong()

	if err != nil {
		return
	}

	recordVersion, err = con.readInt()

	return
}

// utility fn to create cluster record by name
func (db *Database) CreateClusterRecord(cluster_name string, data interface{}, record_type int8, async bool) (rid *orecord.RID, record_version int32, err error) {
	log_debug("CreateClusterRecord")

	// encode data
	byte_data, err := orecord.EncodeFromStruct(data)

	if err != nil {
		return nil, 0, err
	}

	// find cluster
	cluster, exists := db.Clusters[cluster_name]

	if exists == false {
		return nil, 0, errors.New("Could not find cluster with name : " + cluster_name)
	}

	// set synchronization mode
	var sync_mode int8 = Modes.SYNC

	if async {
		sync_mode = Modes.ASYNC
	}

	// -1 is default datasegment id
	cluster_position, record_version, err := db.RecordCreate(-1, cluster.Id, byte_data, record_type, sync_mode)

	if err != nil {
		return nil, 0, err
	}

	rid = orecord.NewRidByExactPosition(cluster.Id, cluster_position)

	return rid, record_version, nil
}
