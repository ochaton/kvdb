package kvdb

import (
	"sync"
)

type T struct {
	mu     sync.RWMutex
	spaces map[string]Space
	closed bool
	wr     writer
}

type GetSpace func(name string) *Space
type applyTxnFunc func(txn *operation) (uint64, error)

// Open opens a new database at the given path.
// If the database does not exist, it will be created.
func Open(path string) (*T, error) {
	db := &T{}
	db.spaces = make(map[string]Space)

	var err error

	db.wr = newWriter(path)

	if err = db.wr.Load(db.applyTxn); err != nil {
		return nil, err
	}

	if err = db.wr.Start(); err != nil {
		_ = db.wr.Close()
		return nil, err
	}

	return db, nil
}

// Space returns the space with the given name.
// If the space does not exist, it returns nil.
func (db *T) Space(name string) (*Space, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}
	return db.space(name, false), nil
}

// NewSpace creates a new space with the given name.
// or returns the existing space if it already exists.
func (db *T) NewSpace(name string) (*Space, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}
	return db.space(name, true), nil
}

func (db *T) Snapshot() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	spaces := map[string]Space{}
	for name, space := range db.spaces {
		spaces[name] = space.View()
	}

	// Send to do snapshot
	db.wr.Snapshot(&spaces)

	return nil
}

func (db *T) Update(txn func(f GetSpace) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}
	return txn(db.getSpaceInner)
}

func (db *T) View(txn func(f GetSpace) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return ErrClosed
	}
	return txn(db.getSpaceInner)
}

// Close closes the database and releases all resources.
func (db *T) Close() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}
	err = db.wr.Close()
	db.closed = true
	db.spaces = nil
	return
}

// called only on load, so we dont need additional locks
func (db *T) applyTxn(txn *operation) (uint64, error) {
	txn.Record.LSN = txn.LSN
	switch txn.Op {
	case OPERATION_SET:
		space := db.space(txn.Record.Tag, true)
		if _, err := space.treeSet(txn.Record); err != nil {
			return 0, err
		}
	case OPERATION_DEL:
		if space := db.space(txn.Record.Tag, false); space != nil {
			if _, err := space.treeDel(txn.Record); err != nil {
				return 0, err
			}
		}
	default:
		return 0, ErrOperationUnknownType
	}

	return txn.LSN, nil
}

func (db *T) space(name string, create bool) *Space {
	if sp, ok := db.spaces[name]; ok {
		return &sp
	}
	if !create {
		return nil
	}
	sp := newSpace(name, db.wr)
	db.spaces[name] = sp
	return &sp
}

func (db *T) getSpaceInner(name string) *Space {
	return db.space(name, false)
}
