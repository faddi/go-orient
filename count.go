package orient

func (o *orientConnection) Count(cluster_name string) (int64, error) {
	//TODO: COUNT (DEPRECATED: USE REQUEST_DATACLUSTER_COUNT)

	// header
	o.beginRequest(opcodes.COUNT)

	o.writeString(cluster_name)

	o.Flush()

	return o.countResponse()
}

func (o *orientConnection) countResponse() (count int64, err error) {

	_, err = o.readResponseHead()

	if err != nil {
		return
	}

	count, err = o.readLong()

	return
}
