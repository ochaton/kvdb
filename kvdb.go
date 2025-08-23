package kvdb

import (
	"errors"

	"github.com/ochaton/kvdb/locks"
	"github.com/ochaton/kvdb/memory_storage"
	"github.com/ochaton/kvdb/writer"
	"github.com/ochaton/kvdb/writer/operation"
)

type DB struct {
	path          string
	memoryStorage memory_storage.MemoryStorage
	lockManager   locks.LockManager
	writer        writer.Writer
}

// Open opens a new database at the given path.
// If the database does not exist, it will be created.
func Open(path string) (*DB, error) {
	db := &DB{
		path:          path,
		memoryStorage: memory_storage.New(),
		lockManager:   locks.NewLockManager(),
		writer:        writer.NewWriter(path),
	}

	if err := db.writer.Load(db.applyTxn); err != nil {
		db.memoryStorage.Clear()
		return nil, err
	}

	if err := db.writer.Start(); err != nil {
		stopErr := db.Close()
		return nil, errors.Join(err, stopErr)
	}

	return db, nil
}

// Close closes the database and releases all resources.
func (db *DB) Close() error {
	err := db.writer.Stop()
	db.memoryStorage.Clear()
	return err
}

func (db *DB) Snapshot() error {
	return db.writer.Snapshot(db.memoryStorage.Snapshot())
}

func (db *DB) Tx(fn func(exec Executer) error) error {
	txExec := NewTXExecuter(db)
	err := fn(txExec)
	if err != nil {
		rErr := txExec.Rollback()
		return errors.Join(err, rErr)
	}
	return txExec.Commit()
}

// called only on load, so we dont need additional locks
func (db *DB) applyTxn(txn *operation.Operation) (uint64, error) {
	txn.Record.LSN = txn.LSN
	txn.Record.Time = txn.Time
	switch txn.Op {
	case operation.OPERATION_SET:
		if err := db.memoryStorage.Set(txn.Record); err != nil {
			return 0, err
		}
	case operation.OPERATION_DEL:
		if err := db.memoryStorage.Del(txn.Record); err != nil {
			return 0, err
		}
	default:
		return 0, ErrOperationUnknownType
	}

	return txn.LSN, nil
}

func (db *DB) GetPath() string {
	return db.path
}

func GetSpace[T any](exec Executer, name string) *Space[T] {
	return newSpace[T](name, exec)
}
