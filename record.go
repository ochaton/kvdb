package kvdb

import "encoding/json"

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
