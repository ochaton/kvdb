package kvdb

import (
	"errors"
	"log"
	"sync"

	"golang.org/x/exp/maps"
)

type KVDB struct {
	mu     sync.RWMutex
	spaces map[string]space
	closed bool
	wr     *writer
}

func Open(path string) (*KVDB, error) {
	db := &KVDB{}
	db.spaces = make(map[string]space)

	var err error
	db.wr = &writer{
		dir:      path,
		incoming: make(chan message, 100),
	}

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

type Stats struct {
	Alive    uint64
	Dead     uint64
	AlivePct float64
}

func (db *KVDB) totalStats() Stats {
	total := Stats{}
	for _, v := range db.spaces {
		total.Alive += v.Alive()
		total.Dead += v.Dead()
	}
	total.AlivePct = float64(total.Alive+1) / float64(total.Alive+1+total.Dead)
	return total
}

func (db *KVDB) TotalStats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.totalStats()
}

// Runs the compaction process on all spaces.
// db will be blocked for creating mvcc snapshot
// and then will be unlocked
func (db *KVDB) Compact() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}

	lsn := db.wr.LSN()
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
	defer maps.Clear(snap)

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

func (db *KVDB) Close() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	db.wr.Close()

	db.closed = true
	db.spaces = nil
	return
}

func (db *KVDB) applyTxn(txn *operation) (uint64, error) {
	txn.Record.LSN = txn.LSN
	switch txn.Op {
	case set:
		db.space(txn.Record.Tag, true).TreeSet(txn.Record)
	case del:
		space := db.space(txn.Record.Tag, false)
		if space == nil {
			break
		}
		space.TreeDel(txn.Record)
	default:
		return 0, errors.New("unknown operation type")
	}

	return txn.LSN, nil
}

func (db *KVDB) space(name string, create bool) *space {
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

func (db *KVDB) Space(name string) Space {
	db.mu.RLock()
	defer db.mu.RUnlock()

	sp, ok := db.spaces[name]
	if !ok {
		return nil
	}
	return newImpl(sp)
}

func (db *KVDB) NewSpace(name string) Space {
	sp := db.space(name, true)
	return newImpl(*sp)
}
