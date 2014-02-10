package orient

import (
	"github.com/faddi/go-orient/orecord"
)

/* OLD: simplify cluster record creation and deletion */
type DatabaseCluster struct {
	db      *Database
	cluster *Cluster
}

func (c *DatabaseCluster) CreateRecord(mode int8, structData interface{}) (clusterPosition int64, recordVersion int32, err error) {

	bytes, err := orecord.EncodeFromStruct(structData)

	if err != nil {
		return
	}

	return c.db.RecordCreate(-1, c.cluster.Id, bytes, RecordTypes.DOCUMENT, mode)
}

// get cluster by name
func (db *Database) DatabaseCluster(name string) (*DatabaseCluster, bool) {

	if cluster, found := db.Clusters[name]; found == true {
		return &DatabaseCluster{db, &cluster}, true
	}

	return nil, false
}
