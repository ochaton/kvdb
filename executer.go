package kvdb

import (
	"github.com/ochaton/kvdb/common/record"
	"github.com/ochaton/kvdb/writer/operation"
)

type Executer interface {
	Set(space string, key []byte, value any) error
	Del(space string, key []byte) error
	Get(space string, key []byte) (*record.Record, error)
	Len(space string) int
	Stream(space string) (chan *record.Record, error)
}

/* default executor */
func (db *DB) Set(space string, key []byte, value any) error {
	rec := record.NewRecord(space, key, value)
	op := operation.NewOperation(rec, operation.OPERATION_SET)
	if err := db.writer.Write(op); err != nil {
		return err
	}
	if err := db.memoryStorage.Set(rec); err != nil {
		return err
	}
	return nil
}

func (db *DB) Del(space string, key []byte) error {
	rec := record.NewRecord(space, key, nil)
	op := operation.NewOperation(rec, operation.OPERATION_DEL)
	if err := db.writer.Write(op); err != nil {
		return err
	}
	if err := db.memoryStorage.Del(rec); err != nil {
		return err
	}
	return nil
}

func (db *DB) Get(space string, key []byte) (*record.Record, error) {
	if key == nil {
		return nil, ErrKeyIsNil
	}
	rec, found := db.memoryStorage.Get(record.NewRecord(space, key, nil))
	if !found {
		return nil, ErrNotFound
	}
	return rec, nil
}

func (db *DB) Len(space string) int {
	return db.memoryStorage.Len(space)
}

func (db *DB) Stream(space string) (chan *record.Record, error) {
	out := make(chan *record.Record)
	iter, err := db.memoryStorage.Iter(space)
	if err != nil {
		return nil, err
	}
	if !iter.IsFinished() {
		go func(output chan *record.Record) {
			defer close(output)
			for iter.Scan() {
				output <- iter.Item()
			}
		}(out)
	} else {
		close(out)
	}

	return out, nil
}

func (db *DB) ModifyBatch(toDel, toSet []*record.Record) error {
	ops := operation.NewEmptyOperationBatch()
	ops.Add(toDel, operation.OPERATION_DEL)
	ops.Add(toSet, operation.OPERATION_SET)
	if err := db.writer.WriteBatch(ops); err != nil {
		return err
	}
	if err := db.memoryStorage.ModifyBatch(toDel, toSet); err != nil {
		return err
	}
	return nil
}
