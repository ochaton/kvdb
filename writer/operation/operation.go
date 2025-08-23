package operation

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ochaton/kvdb/common/record"
)

// operation type
type oType string

const (
	OPERATION_SET oType = "set"
	OPERATION_DEL oType = "del"
)

func (o oType) MarshalJSON() ([]byte, error) {
	switch o {
	case OPERATION_SET:
		return []byte(`"set"`), nil
	case OPERATION_DEL:
		return []byte(`"del"`), nil
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
	default:
		return errors.New("unknown operation type")
	}
	return nil
}

// operation is what actually stored in the log file
type Operation struct {
	LSN    uint64         `json:"lsn"`
	Op     oType          `json:"op"`
	Time   int64          `json:"time"` // unix timestamp
	Record *record.Record `json:"record"`
}

func NewOperation(r *record.Record, op oType) *Operation {
	return &Operation{
		Op:     op,
		Time:   time.Now().Unix(),
		Record: r,
	}
}

func (op *Operation) SetLSN(lsn uint64) {
	op.LSN = lsn
	if op.Record == nil {
		return
	}
	op.Record.LSN = op.LSN
	op.Record.Time = op.Time
}

func (op *Operation) ToBytes() ([]byte, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}

	data = append(data, '\n')
	return data, nil
}

type OperationBatch struct {
	inner []*Operation
}

func NewEmptyOperationBatch() *OperationBatch {
	return &OperationBatch{inner: make([]*Operation, 0)}
}

func NewOperationBatch(records []*record.Record, op oType) *OperationBatch {
	inner := make([]*Operation, 0, len(records))
	for _, r := range records {
		inner = append(inner, &Operation{
			LSN:    r.LSN,
			Op:     op,
			Time:   r.Time,
			Record: r,
		})
	}
	return &OperationBatch{inner: inner}
}

func (ops *OperationBatch) Add(records []*record.Record, op oType) {
	for _, r := range records {
		ops.inner = append(ops.inner, &Operation{
			LSN:    r.LSN,
			Op:     op,
			Time:   r.Time,
			Record: r,
		})
	}
}

func (ops *OperationBatch) ToBytes() ([]byte, error) {
	res := make([]byte, 0, len(ops.inner))
	for _, op := range ops.inner {
		data, err := op.ToBytes()
		if err != nil {
			return nil, err
		}
		res = append(res, data...)
	}
	return res, nil
}

func (ops *OperationBatch) SetLSN(lsn uint64) uint64 {
	for i := range len(ops.inner) {
		ops.inner[i].SetLSN(lsn)
		if i != len(ops.inner)-1 {
			lsn++
		}
	}
	return lsn
}
