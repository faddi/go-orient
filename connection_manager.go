package orient

import (
	"fmt"
)

type connectionManager struct {
	maxConnections int
	conChan        chan *orientConnection
}

// http://www.ryanday.net/2012/09/12/golang-using-channels-for-a-connection-pool/
func (db *Database) initConnectionManager(maxConnections int, adr_str, db_type, db_name, username, password, clientId string) error {

	cm := new(connectionManager)
	cm.maxConnections = maxConnections

	cm.conChan = make(chan *orientConnection)

	first_err := make(chan error)

	go func() {

		for i := 0; i < maxConnections; i++ {

			log_info("creating new connection in pool")
			o := new(orientConnection)

			err := o.orientConnect(adr_str)

			if err != nil {
				log_error("connect failed")
				o.connection.Close()

				if i == 0 {
					first_err <- err
				}
				return
			}

			err = o.dBOpen(db_type, db_name, username, password, clientId)

			if err != nil {
				log_error("open failed")
				if i == 0 {
					first_err <- err
				}
				return
			}

			clusters, err := o.DBOpenResponse()

			if err != nil {
				log_error("failed to read dbopenresponse")
				o.connection.Close()
				if i == 0 {
					first_err <- err
				}
				return
			}

			db.mutex.Lock()
			db.Clusters = clusters
			log_debug(fmt.Sprintf("setting db clusters : %v", db.Clusters))
			db.mutex.Unlock()

			if i == 0 {
				first_err <- nil
			}

			log_info("connection constructed")
			cm.conChan <- o
		}
	}()

	err := <-first_err

	if err != nil {
		return err
	}

	db.connectionManager = cm

	return nil

}

func (cm *connectionManager) getCon() *orientConnection {
	log_info("fetching new connection")
	return <-cm.conChan
}

func (cm *connectionManager) releaseCon(con *orientConnection) {
	log_info("releasing current connection")
	go func() { cm.conChan <- con }()
}
