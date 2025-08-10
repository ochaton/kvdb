package kvdb

import (
	"bytes"
	"errors"
	"sync"

	"github.com/ochaton/kvdb/common/record"
	"github.com/tidwall/btree"
)

type memoryStorage struct {
	inner map[string]*btree.BTreeG[*record.Record]
	mu    sync.RWMutex
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{inner: make(map[string]*btree.BTreeG[*record.Record])}
}

func (ms *memoryStorage) getSpaceTree(space string, create bool) *btree.BTreeG[*record.Record] {
	if tree, ok := ms.inner[space]; ok {
		return tree
	}
	if !create {
		return nil
	}
	tree := btree.NewBTreeG(func(a, b *record.Record) bool {
		return bytes.Compare(a.Key, b.Key) < 0
	})
	ms.inner[space] = tree
	return tree
}

func (ms *memoryStorage) set(rec *record.Record) (*record.Record, error) {
	if rec == nil {
		return nil, ErrRecordIsNil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	tree := ms.getSpaceTree(rec.Space, true)
	prev, _ := tree.Set(rec)
	return prev, nil
}

func (ms *memoryStorage) del(rec *record.Record) (*record.Record, error) {
	if rec == nil {
		return nil, ErrRecordIsNil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if tree := ms.getSpaceTree(rec.Space, false); tree != nil {
		prev, _ := tree.Delete(rec)
		return prev, nil
	}
	return nil, nil
}

func (ms *memoryStorage) get(rec *record.Record) (*record.Record, bool) {
	if rec == nil {
		return nil, false
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if tree := ms.getSpaceTree(rec.Space, false); tree != nil {
		return tree.Get(rec)
	}
	return nil, false
}

func (ms *memoryStorage) len(space string) int {
	if tree := ms.getSpaceTree(space, false); tree != nil {
		return tree.Len()
	}
	return 0
}

func (ms *memoryStorage) iter(space string) (*spaceIterator[record.Record], error) {
	if tree := ms.getSpaceTree(space, false); tree != nil {
		return NewSpaceIterator(tree), nil
	}
	return nil, errors.New("no space")
}

func (ms *memoryStorage) modifyBatch(toDel, toSet []*record.Record) error {
	for _, item := range toDel {
		if _, err := ms.del(item); err != nil {
			return err
		}
	}
	for _, item := range toSet {
		if _, err := ms.set(item); err != nil {
			return err
		}
	}
	return nil
}

type spaceIterator[T any] struct {
	iter     btree.IterG[*T]
	finished bool
}

func NewSpaceIterator[T any](tree *btree.BTreeG[*T]) *spaceIterator[T] {
	iter := tree.Iter()
	return &spaceIterator[T]{iter, !iter.First()}
}

func (sIt *spaceIterator[T]) hasNext() bool {
	return !sIt.finished
}

func (sIt *spaceIterator[T]) next() *T {
	if sIt.finished {
		return nil
	}

	item := sIt.iter.Item()
	if !sIt.iter.Next() || item == nil {
		sIt.finished = true
	}
	return item
}

func (sIt *spaceIterator[T]) collectNext(size int) []*T {
	records := make([]*T, 0, size)
	for len(records) < size {
		record := sIt.next()
		if record == nil {
			break
		}
		records = append(records, record)
	}
	return records
}

func (sIt *spaceIterator[T]) release() {
	sIt.iter.Release()
}

type snapshot[T any] struct {
	iters       []*spaceIterator[T]
	currDataIdx int
}

func (ms *memoryStorage) newSnapshot() *snapshot[record.Record] {
	snap := &snapshot[record.Record]{
		iters:       make([]*spaceIterator[record.Record], 0, len(ms.inner)),
		currDataIdx: 0,
	}
	for _, tree := range ms.inner {
		if tree.Len() == 0 {
			continue
		}
		iterInner := tree.Copy().Iter()
		iter := &spaceIterator[record.Record]{iterInner, !iterInner.First()}
		if iter.finished {
			continue
		}
		snap.iters = append(snap.iters, iter)
	}
	return snap
}
func (sn *snapshot[T]) isFinished() bool {
	return sn.currDataIdx >= len(sn.iters)
}

func (sn *snapshot[T]) GetNext(count int) []*T {
	if sn.isFinished() {
		return []*T{}
	}
	recs := sn.iters[sn.currDataIdx].collectNext(count)
	if sn.iters[sn.currDataIdx].finished {
		sn.iters[sn.currDataIdx].release()
		sn.currDataIdx++
	}
	return recs
}
