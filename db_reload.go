package orient

func (db *Database) ReloadClusterInfo() error {

	con := db.connectionManager.getCon()
	defer db.connectionManager.releaseCon(con)

	con.beginRequest(opcodes.DB_RELOAD)

	con.Flush()

	_, err := con.readResponseHead()

	if err != nil {
		return err
	}

	clusters, err := readClusterInfo(con)

	if err != nil {
		return err
	}

	db.mutex.Lock()
	db.Clusters = clusters
	db.mutex.Unlock()

	return nil
}

// move this to some better place
func readClusterInfo(con *orientConnection) (map[string]Cluster, error) {

	num_clusters, err := con.readShort()

	if err != nil {
		return nil, err
	}

	clusters := make(map[string]Cluster)

	for i := int16(0); i < num_clusters; i++ {

		clustername, err := con.readString()

		if err != nil {
			return nil, err
		}

		clusterid, err := con.readShort()

		if err != nil {
			return nil, err
		}

		clustertype, err := con.readString()

		if err != nil {
			return nil, err
		}

		clustersegmentid, err := con.readShort()

		if err != nil {
			return nil, err
		}

		clusters[clustername] = Cluster{clusterid, clustername, clustertype, clustersegmentid}
	}

	return clusters, nil
}
