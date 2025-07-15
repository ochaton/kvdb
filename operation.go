package kvdb

import (
	"encoding/json"
	"errors"
	"time"
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
	OPERATION_SET oType = "set"
	OPERATION_DEL oType = "del"
	begin         oType = "begin"
	commit        oType = "commit"
	rollback      oType = "rollback"
)

func newOperation(r *record, op oType) operation {
	return operation{
		Op:     op,
		Time:   time.Now().Unix(),
		Record: r,
	}
}

func operationsFromRecords(records []*record, op oType) []*operation {
	operations := make([]*operation, 0, len(records))
	for _, r := range records {
		operations = append(operations, &operation{
			LSN:    r.LSN,
			Op:     op,
			Time:   r.Time,
			Record: r,
		})
	}
	return operations
}

func (op *operation) upgradeRecord() {
	if op.Record == nil {
		return
	}
	op.Record.LSN = op.LSN
	op.Record.Time = op.Time
}

func (o oType) MarshalJSON() ([]byte, error) {
	switch o {
	case OPERATION_SET:
		return []byte(`"set"`), nil
	case OPERATION_DEL:
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
		*o = OPERATION_SET
	case "del":
		*o = OPERATION_DEL
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
