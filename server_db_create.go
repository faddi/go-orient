package orient

func (s *Server) DBCreate(dbname, dbtype, storage_type string) error {
	con := s.con

	// header
	con.beginRequest(opcodes.DB_CREATE)

	con.writeString(dbname)
	con.writeString(dbtype)
	con.writeString(storage_type)

	con.Flush()

	return s.dBCreateResponse(con)
}

func (s *Server) dBCreateResponse(con *orientConnection) error {

	_, err := con.readResponseHead()

	if err != nil {
		return err
	}

	return nil
}
