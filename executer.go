package kvdb

import (
	"github.com/ochaton/kvdb/common/record"
	"github.com/ochaton/kvdb/writer/operation"
)

type Executer interface {
	Set(spaceName string, key []byte, value any) error
	Del(spaceName string, key []byte) error
	Get(spaceName string, key []byte) (*record.Record, bool)
	Len(spaceName string) int
	Stream(spaceName string) (chan *record.Record, error)
}

/* default executor */
func (db *DB) Set(space string, key []byte, value any) error {
	rec := record.NewRecord(space, key, value)
	op := operation.NewOperation(rec, operation.OPERATION_SET)
	if err := db.writer.Write(op); err != nil {
		return err
	}

	_, _ = db.memoryStorage.set(rec)
	return nil
}

func (db *DB) Del(space string, key []byte) error {
	rec := record.NewRecord(space, key, nil)
	op := operation.NewOperation(rec, operation.OPERATION_DEL)
	if err := db.writer.Write(op); err != nil {
		return err
	}

	_, _ = db.memoryStorage.del(rec)
	return nil
}

func (db *DB) Get(space string, key []byte) (*record.Record, bool) {
	rec := record.NewRecord(space, key, nil)
	return db.memoryStorage.get(rec)
}

func (db *DB) Len(space string) int {
	return db.memoryStorage.len(space)
}

func (db *DB) Stream(space string) (chan *record.Record, error) {
	out := make(chan *record.Record)
	iter, err := db.memoryStorage.iter(space)
	if err != nil {
		return nil, err
	}
	if !iter.finished {
		go func(output chan *record.Record) {
			defer close(output)
			defer iter.release()
			for iter.hasNext() {
				output <- iter.next()
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

	if err := db.memoryStorage.modifyBatch(toDel, toSet); err != nil {
		return err
	}
	return nil
}
