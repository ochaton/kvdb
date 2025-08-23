package tree_iterator

import "github.com/tidwall/btree"

type TreeIterator[T any] struct {
	inner      btree.IterG[*T]
	isStarted  bool
	isFinished bool
}

func NewTreeIterator[T any](tree *btree.BTreeG[*T]) *TreeIterator[T] {
	inner := tree.Iter()
	return &TreeIterator[T]{inner, false, false}
}

func (iter *TreeIterator[T]) Release() {
	iter.isFinished = true
	iter.inner.Release()
}

func (iter *TreeIterator[T]) IsFinished() bool {
	return iter.isFinished
}

func (iter *TreeIterator[T]) Scan() bool {
	if !iter.isStarted {
		iter.isStarted = true
		if !iter.inner.First() {
			iter.Release()
			return false
		}
		return true
	}
	if iter.isFinished {
		return false
	}
	if !iter.inner.Next() {
		iter.Release()
		return false
	}
	return true
}

func (iter *TreeIterator[T]) Item() *T {
	return iter.inner.Item()
}

func (iter *TreeIterator[T]) CollectItems(count int) []*T {
	collected := make([]*T, 0, count)
	for iter.Scan() {
		if count <= 0 {
			break
		}
		count--
		collected = append(collected, iter.inner.Item())
	}
	return collected
}
