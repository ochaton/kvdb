package memory_storage

import (
	"bytes"
	"sync"

	"github.com/ochaton/kvdb/common/record"
	"github.com/ochaton/kvdb/common/tree_iterator"
	"github.com/tidwall/btree"
)

type MemoryStorage interface {
	Set(rec *record.Record) error
	Del(rec *record.Record) error
	Get(rec *record.Record) (*record.Record, bool)
	Len(space string) int
	Iter(space string) (*tree_iterator.TreeIterator[record.Record], error)
	ModifyBatch(toDel, toSet []*record.Record) error
	Snapshot() *MemorySnapshot
	Clear()
}

type DefaultMemoryStorage struct {
	inner sync.Map
}

func New() *DefaultMemoryStorage {
	return &DefaultMemoryStorage{
		inner: sync.Map{},
	}
}

func (ms *DefaultMemoryStorage) getSpaceTree(space string, create bool) *btree.BTreeG[*record.Record] {
	if tree, ok := ms.inner.Load(space); ok {
		return tree.(*btree.BTreeG[*record.Record])
	}
	if create {
		tree := btree.NewBTreeG(func(a, b *record.Record) bool {
			return bytes.Compare(a.Key, b.Key) < 0
		})
		actualTree, _ := ms.inner.LoadOrStore(space, tree)
		return actualTree.(*btree.BTreeG[*record.Record])
	}
	return nil
}

func (ms *DefaultMemoryStorage) Set(rec *record.Record) error {
	if rec == nil {
		return ErrRecordIsNil
	}
	ms.getSpaceTree(rec.Space, true).Set(rec)
	return nil
}

func (ms *DefaultMemoryStorage) Del(rec *record.Record) error {
	if rec == nil {
		return ErrRecordIsNil
	}
	if tree := ms.getSpaceTree(rec.Space, false); tree != nil {
		tree.Delete(rec)
	}
	return nil
}

func (ms *DefaultMemoryStorage) Get(rec *record.Record) (*record.Record, bool) {
	if rec == nil {
		return nil, false
	}
	if tree := ms.getSpaceTree(rec.Space, false); tree != nil {
		if rec, found := tree.Get(rec); found {
			return rec, true
		}
	}
	return nil, false
}

func (ms *DefaultMemoryStorage) Len(space string) int {
	if tree := ms.getSpaceTree(space, false); tree != nil {
		return tree.Len()
	}
	return 0
}

func (ms *DefaultMemoryStorage) Iter(space string) (*tree_iterator.TreeIterator[record.Record], error) {
	if tree := ms.getSpaceTree(space, false); tree != nil {
		return tree_iterator.NewTreeIterator(tree), nil
	}
	return nil, ErrSpaceNotFound
}

func (ms *DefaultMemoryStorage) ModifyBatch(toDel, toSet []*record.Record) error {
	for _, item := range toDel {
		if err := ms.Del(item); err != nil {
			return err
		}
	}
	for _, item := range toSet {
		if err := ms.Set(item); err != nil {
			return err
		}
	}
	return nil
}

func (ms *DefaultMemoryStorage) Snapshot() *MemorySnapshot {
	snap := NewEmptyMemorySnapshot()
	ms.inner.Range(func(space, tree any) bool {
		snap.AddTree(tree.(*btree.BTreeG[*record.Record]).Copy())
		return true
	})
	return snap
}

func (ms *DefaultMemoryStorage) Clear() {
	ms.inner.Clear()
}
