package record

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
type Record struct {
	LSN   uint64 `json:"-"`
	Time  int64  `json:"-"` // unix timestamp
	Space string `json:"space"`
	Key   []byte `json:"key"`
	Value any    `json:"value"`
}

func NewRecord(space string, key []byte, value any) *Record {
	return &Record{
		Key:   key,
		Value: value,
		Space: space,
	}
}

func (r *Record) MarshalJSON() ([]byte, error) {
	v, err := json.Marshal(r.Value)
	if err != nil {
		return nil, err
	}
	k, err := json.Marshal(string(r.Key))
	if err != nil {
		return nil, err
	}

	s := []byte(`{"space":"` + r.Space + `","key":`)
	s = append(s, k...)
	s = append(s, `,"value":`...)
	s = append(s, v...)
	s = append(s, "}"...)

	return s, nil
}

func (r *Record) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Space string `json:"space"`
		Key   string `json:"key"`
		Value any    `json:"value"`
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	r.Space = tmp.Space
	r.Key = []byte(tmp.Key)
	r.Value = tmp.Value

	return nil
}

func (r *Record) Into(into any) error {
	/* TODO: return after creating schema
	if reflect.TypeOf(r.Value) != reflect.ValueOf(into).Elem().Type() {
		fmt.Printf("%v and %v\n", reflect.TypeOf(r.Value), reflect.ValueOf(into).Elem().Type())
		return ErrIntoInvalidType
	}
	*/

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
