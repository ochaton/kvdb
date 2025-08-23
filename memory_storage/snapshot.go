package memory_storage

import (
	"github.com/ochaton/kvdb/common/record"
	"github.com/ochaton/kvdb/common/tree_iterator"
	"github.com/tidwall/btree"
)

type MemorySnapshot struct {
	iters []*tree_iterator.TreeIterator[record.Record]
	cIdx  int
}

func NewEmptyMemorySnapshot() *MemorySnapshot {
	return &MemorySnapshot{
		iters: make([]*tree_iterator.TreeIterator[record.Record], 0),
		cIdx:  0,
	}
}
func (sn *MemorySnapshot) AddTree(treeCopy *btree.BTreeG[*record.Record]) {
	if treeCopy.Len() == 0 {
		return
	}
	sn.iters = append(sn.iters, tree_iterator.NewTreeIterator(treeCopy))
}

func (sn *MemorySnapshot) IsFinished() bool {
	return sn.cIdx >= len(sn.iters)
}

func (sn *MemorySnapshot) GetItems(count int) []*record.Record {
	if sn.IsFinished() {
		return []*record.Record{}
	}

	collected := make([]*record.Record, 0, count)
	for len(collected) < count && sn.cIdx < len(sn.iters) {
		if !sn.iters[sn.cIdx].Scan() {
			sn.cIdx++
			continue
		}
		collected = append(collected, sn.iters[sn.cIdx].Item())
	}
	return collected
}
