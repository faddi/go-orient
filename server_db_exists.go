package orient

func (s *Server) DBExists(dbname string) (bool, error) {
	con := s.con

	// header
	con.beginRequest(opcodes.DB_EXIST)

	con.writeString(dbname)

	if protocol_version > 15 {
		con.writeString(StorageTypes.LOCAL)
	}

	con.Flush()

	return s.dBExistsResponse(con)
}

func (s *Server) dBExistsResponse(con *orientConnection) (bool, error) {

	_, err := con.readResponseHead()

	if err != nil {
		return false, err
	}

	b, err := con.readBool()

	if err != nil {
		return false, err
	}

	return b, nil
}
