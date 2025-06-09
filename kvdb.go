package kvdb

import (
	"errors"
	"log"
	"sync"
)

type T struct {
	mu     sync.RWMutex
	spaces map[string]space
	closed bool
	wr     *writer
}

type GetSpace func(name string) Space

// Open opens a new database at the given path.
// If the database does not exist, it will be created.
func Open(path string) (*T, error) {
	db := &T{}
	db.spaces = make(map[string]space)

	var err error
	db.wr = newWriter(path)

	if err = db.wr.Load(db.applyTxn); err != nil {
		_ = db.wr.Close()
		return nil, err
	}

	if err = db.wr.Start(); err != nil {
		_ = db.wr.Close()
		return nil, err
	}

	stats := db.TotalStats()
	log.Printf("db stats: Alive:%d, Dead:%d (defrag: %.2f%%)\n",
		stats.Alive, stats.Dead, stats.AlivePct*100)

	if stats.AlivePct < 0.5 {
		if err = db.Compact(); err != nil {
			_ = db.wr.Close()
			return nil, err
		}
	}

	return db, nil
}

func (db *T) spaceNoLock(name string) Space {
	sp, ok := db.spaces[name]
	if !ok {
		return nil
	}
	return newImpl(sp)
}

// Space returns the space with the given name.
// If the space does not exist, it returns nil.
func (db *T) Space(name string) Space {
	sp := db.space(name, false)
	if sp == nil {
		return nil
	}
	return newImpl(*sp)
}

// NewSpace creates a new space with the given name.
// or returns the existing space if it already exists.
func (db *T) NewSpace(name string) Space {
	sp := db.space(name, true)
	return newImpl(*sp)
}

func (db *T) Update(txn func(f GetSpace) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return txn(db.spaceNoLock)
}

func (db *T) View(txn func(f GetSpace) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return txn(db.spaceNoLock)
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

// Runs the compaction process on all spaces.
// db will be blocked for creating mvcc snapshot
// and then will be unlocked before requesting compaction
func (db *T) Compact() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}

	lsn := db.wr.GetLSN()
	if lsn == 0 {
		db.mu.Unlock()
		return nil
	}

	stats := db.totalStats()

	snap := make(map[string]space, len(db.spaces))
	for k, sp := range db.spaces {
		snap[k] = sp.View()
	}
	db.mu.Unlock()
	defer clear(snap)

	// now we can compact all spaces
	err := db.wr.Compact(snap, lsn)
	if err != nil {
		return err
	}

	// now we need to reset statistics
	for k, sp := range snap {
		space, ok := db.spaces[k]
		if !ok {
			continue
		}
		if !space.Dec(sp.Dead()) {
			log.Printf("failed to reset space '%s' stats", k)
		}
	}

	new_stats := db.TotalStats()
	log.Printf("compaction: %d -> %d (%.2f%%)\n",
		stats.Alive+stats.Dead, new_stats.Alive+new_stats.Dead, new_stats.AlivePct*100)

	return nil
}

func (db *T) applyTxn(txn *operation) (uint64, error) {
	txn.Record.LSN = txn.LSN
	switch txn.Op {
	case set:
		db.space(txn.Record.Tag, true).TreeSet(txn.Record)
	case del:
		if space := db.space(txn.Record.Tag, false); space != nil {
			space.TreeDel(txn.Record)
		}
	default:
		return 0, errors.New("unknown operation type")
	}

	return txn.LSN, nil
}

func (db *T) space(name string, create bool) *space {
	db.mu.RLock()
	sp, ok := db.spaces[name]
	db.mu.RUnlock()
	if ok {
		return &sp
	}
	if !create {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	sp = newSpace(name, db.wr)
	db.spaces[name] = sp
	return &sp
}
