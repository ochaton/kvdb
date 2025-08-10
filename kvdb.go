package kvdb

import (
	"sync"

	"github.com/ochaton/kvdb/locks"
	"github.com/ochaton/kvdb/writer"
	"github.com/ochaton/kvdb/writer/operation"
)

type DB struct {
	path          string
	closed        bool
	memoryStorage *memoryStorage
	lockManager   *locks.LockManager
	writer        writer.Writer
	mu            sync.RWMutex
}

// Open opens a new database at the given path.
// If the database does not exist, it will be created.
func Open(path string) (*DB, error) {
	db := &DB{
		path:          path,
		memoryStorage: newMemoryStorage(),
		lockManager:   locks.NewLockManager(),
		writer:        writer.NewWriter(path),
		mu:            sync.RWMutex{},
	}

	if err := db.writer.Load(db.applyTxn); err != nil {
		return nil, err
	}

	if err := db.writer.Start(); err != nil {
		_ = db.writer.Close()
		return nil, err
	}

	return db, nil
}

// Close closes the database and releases all resources.
func (db *DB) Close() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	err = db.writer.Close()
	db.closed = true
	db.memoryStorage = nil
	return
}

func (db *DB) Snapshot() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}

	return db.writer.Snapshot(db.memoryStorage.newSnapshot())
}

func (db *DB) Update(fn func(exec Executer) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return fn(db)
}

func (db *DB) View(fn func(exec Executer) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return fn(db)
}

func (db *DB) Tx(fn func(exec Executer) error) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	db.mu.RUnlock()
	txExec := NewTXExecuter(db, db.lockManager)
	err := fn(txExec)
	if err != nil {
		txExec.Rollback()
		return err
	}
	return txExec.Commit()
}

// called only on load, so we dont need additional locks
func (db *DB) applyTxn(txn *operation.Operation) (uint64, error) {
	txn.Record.LSN = txn.LSN
	txn.Record.Time = txn.Time
	switch txn.Op {
	case operation.OPERATION_SET:
		if _, err := db.memoryStorage.set(txn.Record); err != nil {
			return 0, err
		}
	case operation.OPERATION_DEL:
		if _, err := db.memoryStorage.del(txn.Record); err != nil {
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
