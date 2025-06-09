package kvdb

import (
	"encoding/json"
	"errors"
)

// operation is what actually stored in the log file
type operation struct {
	LSN    uint64  `json:"lsn"`
	Op     oType   `json:"op"`
	Time   int64   `json:"time"` // unix timestamp
	Record *record `json:"record"`
}

// operation type
type oType string

const (
	set      oType = "set"
	del      oType = "del"
	begin    oType = "begin"
	commit   oType = "commit"
	rollback oType = "rollback"
)

func (o oType) MarshalJSON() ([]byte, error) {
	switch o {
	case set:
		return []byte(`"set"`), nil
	case del:
		return []byte(`"del"`), nil
	case begin:
		return []byte(`"begin"`), nil
	case commit:
		return []byte(`"commit"`), nil
	case rollback:
		return []byte(`"rollback"`), nil
	default:
		return nil, errors.New("unknown operation type")
	}
}

func (o *oType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	switch s {
	case "set":
		*o = set
	case "del":
		*o = del
	case "begin":
		*o = begin
	case "commit":
		*o = commit
	case "rollback":
		*o = rollback
	default:
		return errors.New("unknown operation type")
	}
	return nil
}
