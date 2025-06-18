package kvdb

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sync"
)

type Header struct {
	LSN  uint64 `json:"lsn"`
	Time int64  `json:"time"` // unix timestamp
	Key  []byte `json:"key"`
}

var bufPool = &sync.Pool{
	New: func() any {
		return bytes.NewBuffer(nil)
	},
}

// record represents a single record in the btree
type record struct {
	LSN   uint64 `json:"-"`
	Time  int64  `json:"-"` // unix timestamp
	Key   []byte `json:"key"`
	Tag   string `json:"tag"`
	Value any    `json:"value"`
}

func (r *record) MarshalJSON() ([]byte, error) {
	v, err := json.Marshal(r.Value)
	if err != nil {
		return nil, err
	}
	k, err := json.Marshal(string(r.Key))
	if err != nil {
		return nil, err
	}

	s := []byte(`{"tag":"` + r.Tag + `","key":`)
	s = append(s, k...)
	s = append(s, `,"value":`...)
	s = append(s, v...)
	s = append(s, "}"...)

	return s, nil
}

func (r *record) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Key   string `json:"key"`
		Tag   string `json:"tag"`
		Value any    `json:"value"`
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	r.Key = []byte(tmp.Key)
	r.Tag = tmp.Tag
	r.Value = tmp.Value

	return nil
}

func (r *record) into(into any) error {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	buf.Reset()
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	if err := enc.Encode(r.Value); err != nil {
		return err
	}
	if err := dec.Decode(into); err != nil {
		return err
	}

	// if into has field Header type of kvdb.Header
	v := reflect.ValueOf(into).Elem()
	num := v.NumField()

	for i := range num {
		if v.Type().Field(i).Type == reflect.TypeOf(Header{}) {
			v.Field(i).Set(reflect.ValueOf(Header{
				LSN:  r.LSN,
				Time: r.Time,
				Key:  r.Key,
			}))
			break
		}
	}

	return nil
}
