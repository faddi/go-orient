package orient

func (s *Server) DBDrop(dbname string) error {
	con := s.con

	// header
	con.beginRequest(opcodes.DB_DELETE)

	con.writeString(dbname)

	if protocol_version > 15 {
		con.writeString(StorageTypes.LOCAL)
	}

	con.Flush()

	return s.dBDropResponse(con)
}

func (s *Server) dBDropResponse(con *orientConnection) error {

	_, err := con.readResponseHead()

	if err != nil {
		return err
	}

	return nil
}
