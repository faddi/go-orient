package bbtest

import (
	o "github.com/faddi/go-orient"
	. "launchpad.net/gocheck"
)

// database tests
type DeadSuite struct {
	db *o.Database
}

// setup
// get a server connection and create the test database
func (s *DeadSuite) SetUpSuite(c *C) {
	// if the testdatabase already exists drop it and recreate

	// grab a database connection
	db, err := o.DBOpen("127.0.0.1:2424", "graph", "GratefulDeadConcerts", "admin", "admin", "")
	c.Assert(err, IsNil)
	//c.Assert(db.SessionId() > 1, Equals, true) connectionmanager

	s.db = db
}

func (s *DeadSuite) TearDownSuite(c *C) {
	s.db.DBClose()
}

//func (s *DeadSuite) TestHW(c *C) {
//	// todo : add some test to get a tree back
//	//resChan, err := s.db.CreateCommand("select * from E").ExecuteAsyncQuery()
//
//	//c.Assert(err, IsNil)
//	//for _ = range resChan {
//	//	//c.Log(r.Rid)
//	//}
//
//}
