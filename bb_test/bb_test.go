package bbtest

import (
	o "github.com/faddi/go-orient"
	"github.com/faddi/go-orient/orecord"
	. "launchpad.net/gocheck"
	"strconv"
	"testing"
	//            "math/rand"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&DeadSuite{})

// Server tests
type ServerSuite struct {
	server *o.Server
}

var _ = Suite(&ServerSuite{})

const (
	//rootpw     = "9108F0229545C3AFA5DA60F093BB2F6EEAA4F0324D03D58EDEB121D880BC634A"
	rootpw     = "DB7AFA760158678D5F0C988B395E8AD266911CBCF18C302CB96EE80CAD6384B4"
	testdbname = "testdatabase"
)

// setup
// get a server connection and create the test database
// tests exists and create
func (s *ServerSuite) SetUpTest(c *C) {
	server, err := o.Connect("127.0.0.1:2424", "root", rootpw, "")
	c.Assert(err, IsNil)
	c.Assert(server.SessionId() > 1, Equals, true)

	s.server = server

	exists, err := s.server.DBExists(testdbname)
	c.Assert(err, IsNil)

	if exists == false {
		err = s.server.DBCreate(testdbname, o.DatabaseTypes.GRAPH, o.StorageTypes.PLOCAL)
		c.Assert(err, IsNil)
	}
}

func (s *ServerSuite) TestDBDrop(c *C) {

	name := "temporaryDBToDrop"

	// cleanup in case some old test fails to clean up
	exists, err := s.server.DBExists(name)
	c.Assert(err, IsNil)

	if exists == true {
		err = s.server.DBDrop(name)
		c.Assert(err, IsNil)
	}

	// create a new db
	err = s.server.DBCreate(name, o.DatabaseTypes.DOCUMENT, o.StorageTypes.LOCAL)
	c.Assert(err, IsNil)

	// verify that it exists
	exists, err = s.server.DBExists(name)
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)

	// drop it
	err = s.server.DBDrop(name)
	c.Assert(err, IsNil)

	// verify that it no longer exists
	exists, err = s.server.DBExists(name)
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, false)
}

// database tests
type DatabaseSuite struct {
	db *o.Database
}

var _ = Suite(&DatabaseSuite{})

// setup
// get a server connection and create the test database
func (s *DatabaseSuite) SetUpSuite(c *C) {
	// connect to server
	server, err := o.Connect("127.0.0.1:2424", "root", rootpw, "")
	c.Assert(err, IsNil)
	c.Assert(server.SessionId() > 1, Equals, true)

	// if the testdatabase already exists drop it and recreate

	exists, err := server.DBExists(testdbname)
	c.Assert(err, IsNil)

	if exists == true {
		err = server.DBDrop(testdbname)
		c.Assert(err, IsNil)
	}

	err = server.DBCreate(testdbname, o.DatabaseTypes.GRAPH, o.StorageTypes.PLOCAL)
	c.Assert(err, IsNil)

	// grab a database connection
	db, err := o.DBOpen("127.0.0.1:2424", "graph", testdbname, "admin", "admin", "")

	c.Logf("clusters : %v", db.Clusters)

	c.Assert(err, IsNil)
	//c.Assert(db.SessionId() > 1, Equals, true) connectionmanager

	s.db = db
}

func (s *DatabaseSuite) TearDownSuite(c *C) {
	server, err := o.Connect("127.0.0.1:2424", "root", rootpw, "")
	c.Assert(err, IsNil)
	c.Assert(server.SessionId() > 1, Equals, true)

	err = server.DBDrop(testdbname)
	c.Assert(err, IsNil)
}

func (s *DatabaseSuite) TestDBOpen(c *C) {
	c.Assert(s.db, NotNil)
}

func (s *DatabaseSuite) TestSerializedCommandResult(c *C) {

	cluster := "testinsertselect"

	//s.db.CreateCommand("drop cluster " + cluster).ExecuteCommand()
	d, err := s.db.CreateCommand("create cluster " + cluster + " physical").ExecuteCommand()

	c.Assert(err, IsNil)
	c.Assert(d.HasSerializedResult(), Equals, true)

	id, err := strconv.Atoi(d.SerializedResult())

	c.Assert(err, IsNil)
	c.Assert(id > 0, Equals, true)

	res, err := s.db.CreateCommand("drop cluster " + cluster).ExecuteCommand()

	c.Assert(err, IsNil)
	c.Assert(res.HasSerializedResult(), Equals, true)
	c.Assert(res.SerializedResult(), Equals, "true")
}

func (s *DatabaseSuite) TestBatchInsertSelect(c *C) {
	testClass := "testclass"

	ensureClassExists(c, s.db, testClass)

	sql := "insert into " + testClass + "(number) values "
	for i := 0; i < 100; i++ {
		sql += "(" + strconv.Itoa(i) + "),"
	}

	sql = sql[:len(sql)-1]

	cmd := s.db.CreateCommand(sql)
	c.Logf("sending insert\n")
	res, err := cmd.ExecuteCommand()
	c.Logf("got insert response\n")

	c.Assert(err, IsNil)

	cmd = s.db.CreateCommand("select * from " + testClass)

	c.Logf("sending select\n")
	res, err = cmd.ExecuteSyncQuery()
	c.Logf("get select response\n")

	c.Assert(err, IsNil)
	c.Assert(len(res.Records()), Equals, 100)
}

func ensureClassExists(c *C, db *o.Database, class string) {
	if cluster, ok := db.Clusters[class]; ok == true {
		c.Log("dropping existing class")
		_, err := db.CreateCommand("drop class " + cluster.Name).ExecuteCommand()
		c.Log("class dropped")

		c.Assert(err, IsNil)
	}

	c.Log("creating class " + class)
	_, err := db.CreateCommand("create class " + class).ExecuteCommand()
	c.Log("class created")

	c.Assert(err, IsNil)
}

func ensureExtendedClassExists(c *C, db *o.Database, class, target string) {
	if cluster, ok := db.Clusters[class]; ok == true {
		c.Log("dropping existing class")
		_, err := db.CreateCommand("drop class " + cluster.Name).ExecuteCommand()
		c.Log("class dropped")

		c.Assert(err, IsNil)
	}

	c.Log("creating class " + class)
	_, err := db.CreateCommand("create class " + class + " extends " + target).ExecuteCommand()
	c.Log("class created")

	c.Assert(err, IsNil)
}

func (s *DatabaseSuite) TestInsertSelectWithRels(c *C) {

	testClass := "testrels"

	ensureClassExists(c, s.db, testClass)

	var record *orecord.RawRecord = nil

	for i := 0; i < 10; i++ {

		var cmd *o.SQLCommand
		if record == nil {
			cmd = s.db.CreateCommand("insert into " + testClass + "(number) values (" + strconv.Itoa(i) + ")")
		} else {
			cmd = s.db.CreateCommand("insert into " + testClass + "(number, parent) values (" + strconv.Itoa(i) + "," + record.Rid.String() + ")")
		}

		res, err := cmd.ExecuteCommand()

		c.Assert(err, IsNil)
		c.Assert(res.HasRecordsResult(), Equals, true)

		record = res.Records()[0]
	}

	res, err := s.db.CreateCommand("select * from " + testClass).ExecuteSyncQuery()

	c.Assert(err, IsNil)

	for _, r := range res.Records() {
		_, err := r.DecodeToMap()
		c.Assert(err, IsNil)
	}

}

func (s *DatabaseSuite) TestPrefetch(c *C) {

	testClass := "testprefetch"

	ensureClassExists(c, s.db, testClass)

	res, err := s.db.CreateCommand("insert into " + testClass + " set number = 6").ExecuteCommand()
	c.Assert(err, IsNil)

	r := res.Records()[0]
	c.Logf("%v", r.Rid.String())
	m, err := r.DecodeToStringMap()

	c.Assert(err, IsNil)
	c.Logf("%v", m)

	res2, err := s.db.CreateCommand("insert into " + testClass + " (number, stuff) values(1, [" + r.Rid.String() + "])").ExecuteCommand()

	c.Assert(err, IsNil)

	c.Assert(res2.HasRecordsResult(), Equals, true)
	c.Assert(res2.Records(), HasLen, 1)

	r2 := res2.Records()[0]

	m2, err := r2.DecodeToStringMap()

	c.Assert(err, IsNil)
	c.Logf("%v", m2)
}

func (s *DatabaseSuite) TestInsertDecodeComplexObject(c *C) {

	testClass := "testcomplex"

	ensureClassExists(c, s.db, testClass)

	res, err := s.db.CreateCommand("insert into " + testClass + "(obj, astring, astringarr) values ({\"data\":[1,2,3,4]}, \"this is a string\", [\"asd\"])").ExecuteCommand()

	c.Assert(err, IsNil)

	m, err := res.Records()[0].DecodeToMap()

	c.Assert(err, IsNil)

	c.Logf("%v", m)
	c.Assert(m["class"], Equals, testClass)

	data := m["obj"].(map[string]interface{})
	ints := data["data"].([]interface{})
	c.Assert(ints[0].(int32), Equals, int32(1))
	c.Assert(ints[1].(int32), Equals, int32(2))
	c.Assert(ints[2].(int32), Equals, int32(3))
	c.Assert(ints[3].(int32), Equals, int32(4))
	c.Assert(m["astring"], Equals, "this is a string")
	c.Assert(m["astringarr"].([]interface{})[0].(string), Equals, "asd")
}

func (s *DatabaseSuite) TestInsertParametrized(c *C) {

	testClass := "testparametrized"

	ensureClassExists(c, s.db, testClass)

	x := s.db.CreateCommand("insert into " + testClass + "(one, two, three, four ) values (?, ?, ?, ?)")

	x.Params = []interface{}{1, 2, 3, "fourth"}
	res, err := x.ExecuteCommand()

	c.Assert(err, IsNil)

	d, err := res.Records()[0].DecodeToMap()

	c.Logf("%v", d)

	c.Assert(d["one"], Equals, int32(1))
	c.Assert(d["two"], Equals, int32(2))
	c.Assert(d["three"], Equals, int32(3))
	c.Assert(d["four"], Equals, "fourth")
	c.Assert(err, IsNil)
}

//func (s *DatabaseSuite) TestInsertParametrizedComposite(c *C) {
//
//    testClass := "testparametrized"
//
//    ensureClassExists(c, s.db, testClass)
//
//    x := s.db.CreateCommand("insert into " + testClass + "(one,two,three,four) values (:one, :two, :three, :four)")
//    x.Params = []byte{1,1}
//    res, err := x.ExecuteCommand()
//
//    c.Assert(err, IsNil)
//
//    d, err := res.Records()[0].DecodeToMap()
//
//    c.Logf("%v", d)
//
//    c.Assert(d["one"], Equals, int32(1))
//    c.Assert(d["two"], Equals, int32(2))
//    c.Assert(d["three"], Equals, int32(3))
//    c.Assert(d["four"], Equals, int32(4))
//    c.Assert(err, IsNil)
//}

func (s *DatabaseSuite) TestAsyncCommand(c *C) {
	testClass := "testasync"
	ensureClassExists(c, s.db, testClass)

	sql := "insert into " + testClass + " (number) values (1)"

	cmd := s.db.CreateCommand(sql)

	for i := 0; i < 10; i++ {
		res, err := cmd.ExecuteCommand()
		c.Assert(err, IsNil)
		c.Assert(res.HasRecordsResult(), Equals, true)
	}

	cmd = s.db.CreateCommand("select * from " + testClass)

	retchan, err := cmd.ExecuteAsyncQuery()

	c.Assert(err, IsNil)

	var i = 0

	for rec := range retchan {
		i++
		strmap, err := rec.DecodeToStringMap()
		c.Assert(err, IsNil)
		c.Assert(strmap["number"], Equals, "1")
	}

	c.Assert(i, Equals, 10)
}

// create record struct
type createRecordStruct struct {
	Test string
}

func (s *DatabaseSuite) TestCRUDRecord(c *C) {
	testClass := "testcreaterecord"
	ensureClassExists(c, s.db, testClass)

	err := s.db.ReloadClusterInfo()

	c.Assert(err, IsNil)
	inrecord := &createRecordStruct{"Hello sir!"}

	// create a record
	rid, record_version, err := s.db.CreateClusterRecord(testClass, inrecord, o.RecordTypes.DOCUMENT, false)

	c.Assert(err, IsNil)
	c.Assert(rid, NotNil)
	c.Assert(record_version, Equals, int32(0))

	// load it
	r, err := s.db.RecordLoad(rid.ClusterId, rid.ClusterPosition, "", false, false)

	c.Assert(err, IsNil)
	res := &createRecordStruct{}
	err = r.DecodeToStruct(res)
	c.Assert(err, IsNil)
	c.Assert(inrecord.Test, Equals, res.Test)

	donechan := make(chan bool)
	// delete the record async
	s.db.RecordDelete(r.Rid.ClusterId, r.Rid.ClusterPosition, r.Version, true, func(ok bool, err error) {
		c.Assert(err, IsNil)
		c.Assert(ok, Equals, true)
		donechan <- true
	})

	// ... but synchronize anyway
	<-donechan

	// record should be gone now
	r2, err := s.db.RecordLoad(rid.ClusterId, rid.ClusterPosition, "", false, false)
	c.Assert(err, IsNil)
	c.Assert(r2, IsNil)
}

func (s *DatabaseSuite) TestSpecialLinkset(c *C) {
	vertex := "testlinkset_vertex"
	edge := "testlinkset_edge"

	ensureExtendedClassExists(c, s.db, vertex, "V")
	ensureExtendedClassExists(c, s.db, edge, "E")

	res, err := s.db.CreateCommand("create vertex " + vertex + " set type = ?").ExecuteCommand("root")

	c.Assert(err, IsNil)
	c.Assert(res.HasRecordsResult(), Equals, true)
	c.Assert(len(res.Records()), Equals, 1)

	rootRID := res.Records()[0].Rid

	numLinks := 7

	for i := 0; i<numLinks; i++ {
		r, err := s.db.CreateCommand("create vertex " + vertex + " set type = ?").ExecuteCommand("child")
		c.Assert(err, IsNil)
		c.Assert(r.HasRecordsResult(), Equals, true)
		c.Assert(len(r.Records()), Equals, 1)
		myRID := r.Records()[0].Rid

		result, err := s.db.CreateCommand("create edge " + edge + " from " + rootRID.String() + " to " + myRID.String() + " set type = ?").ExecuteCommand("link")

		c.Assert(err, IsNil)
		c.Assert(result.HasRecordsResult(), Equals, true)
		c.Assert(len(result.Records()), Equals, 1)
	}

	res, err = s.db.CreateCommand("select * from " + vertex + " where @rid = #11:0").ExecuteCommand()

	c.Assert(err, IsNil)
	c.Assert(res.HasRecordsResult(), Equals, true)

	for _, rec := range res.Records() {
		m, err := rec.DecodeToMap()
		c.Assert(err, IsNil)
		c.Logf("%v\n", m)
	}

}


















