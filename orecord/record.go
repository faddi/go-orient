package orecord

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// set serialization types (currently not used)
type Set map[interface{}]struct{}

type RawRecord struct {
	Data    []byte
	Version int32
	Rid     *RID
	Type    int8
}

func (r *RawRecord) DecodeToStringMap() (map[string]string, error) {
	return DecodeToStringMap(r.Data)
}

func (r *RawRecord) DecodeToMap() (map[string]interface{}, error) {
	return DecodeToMap(r.Data)
}

func (r *RawRecord) DecodeToStruct(target interface{}) error {
	return DecodeToStruct(r.Data, target)
}

type RID struct {
	ClusterId       int16
	ClusterPosition int64
}

func (r *RID) String() string {
	return fmt.Sprintf("#%d:%d", r.ClusterId, r.ClusterPosition)
}

func NewRid(rid string) (*RID, error) {

	parts := strings.Split(rid, ":")

	if len(parts) != 2 {
		return nil, errors.New("Invalid rid format: " + rid)
	}

	cId, err := strconv.ParseInt(parts[0][1:], 10, 16)

	if err != nil {
		return nil, errors.New("Could not parse clusterId : " + parts[0])
	}

	cPosition, err := strconv.ParseInt(parts[1], 10, 64)

	if err != nil {
		return nil, errors.New("Could not parse clusterPosition : " + parts[1])
	}

	return &RID{int16(cId), cPosition}, nil
}

func NewRidByExactPosition(clusterId int16, clusterPosition int64) *RID {
	return &RID{int16(clusterId), clusterPosition}
}
