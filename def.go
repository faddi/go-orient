package orient

var opcodes = struct {
	SHUTDOWN,
	CONNECT,
	DB_OPEN,
	DB_CREATE,
	DB_CLOSE,
	DB_EXIST,
	DB_DELETE,
	DB_SIZE,
	DB_COUNTRECORDS,
	DATACLUSTER_ADD,
	DATACLUSTER_REMOVE,
	DATACLUSTER_COUNT,
	DATACLUSTER_DATARANGE,
	DATASEGMENT_ADD,
	DATASEGMENT_REMOVE,
	RECORD_LOAD,
	RECORD_CREATE,
	RECORD_UPDATE,
	RECORD_DELETE,
	COUNT,
	COMMAND,
	TX_COMMIT,
	CONFIG_GET,
	CONFIG_SET,
	CONFIG_LIST,
	DB_RELOAD int8
}{
	SHUTDOWN:              1,
	CONNECT:               2,
	DB_OPEN:               3,
	DB_CREATE:             4,
	DB_CLOSE:              5,
	DB_EXIST:              6,
	DB_DELETE:             7,
	DB_SIZE:               8,
	DB_COUNTRECORDS:       9,
	DATACLUSTER_ADD:       10,
	DATACLUSTER_REMOVE:    11,
	DATACLUSTER_COUNT:     12,
	DATACLUSTER_DATARANGE: 13,
	DATASEGMENT_ADD:       20,
	DATASEGMENT_REMOVE:    21,
	RECORD_LOAD:           30,
	RECORD_CREATE:         31,
	RECORD_UPDATE:         32,
	RECORD_DELETE:         33,
	COUNT:                 40,
	COMMAND:               41,
	TX_COMMIT:             60,
	CONFIG_GET:            70,
	CONFIG_SET:            71,
	CONFIG_LIST:           72,
	DB_RELOAD:             73,
}

var CommandModes = struct {
	ASYNC,
	SYNC int8
}{
	ASYNC: 97,  // a async
	SYNC:  115, // s sync
}

var Modes = struct {
	ASYNC,
	SYNC int8
}{
	ASYNC: 1,
	SYNC:  0,
}

var RecordTypes = struct {
	RAW_BYTES,
	FLAT_DATA,
	DOCUMENT int8
}{
	RAW_BYTES: 'b',
	FLAT_DATA: 'f',
	DOCUMENT:  'd',
}

var DataClusterTypes = struct {
	PHYSICAL,
	MEMORY string
}{
	PHYSICAL: "PHYSICAL",
	MEMORY:   "MEMORY",
}

var queryTypes = struct {
	AsyncQuery,
	SyncQuery,
	Command string
}{
	SyncQuery:  "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery",
	AsyncQuery: "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery",
	Command:    "com.orientechnologies.orient.core.sql.OCommandSQL",
}

var DatabaseTypes = struct {
	DOCUMENT,
	GRAPH string
}{
	DOCUMENT: "document",
	GRAPH:    "graph",
}

var StorageTypes = struct {
	REMOTE,
	LOCAL,
	PLOCAL,
	MEMORY string
}{
	REMOTE: "remote",
	LOCAL:  "local",
	PLOCAL: "plocal",
	MEMORY: "memory",
}
