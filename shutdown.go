package orient

func (o *orientConnection) Shutdown(username, password string) error {

	// header
	o.beginRequest(opcodes.SHUTDOWN)

	o.writeString(username)
	o.writeString(password)

	o.Flush()

	return o.shutdownResponse()
}

func (o *orientConnection) shutdownResponse() (err error) {
	_, err = o.readResponseHead()
	return
}
