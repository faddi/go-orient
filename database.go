package orient

import (
	"sync"
)

type Database struct {
	connectionManager *connectionManager
	Clusters          map[string]Cluster
	mutex             sync.Mutex
}
