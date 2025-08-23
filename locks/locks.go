package locks

import (
	"bytes"
)

type lock struct {
	space string
	key   []byte
	owner string
	ch    chan struct{}
}

func newLock(owner, space string, key []byte) *lock {
	return &lock{
		space: space,
		key:   key,
		owner: owner,
		ch:    make(chan struct{}),
	}
}

func (l *lock) IsSpaceLock() bool {
	return l.key == nil
}

func (l *lock) Release() {
	close(l.ch)
}

func lockCompare(a, b *lock) bool {
	if a.space != b.space {
		return a.space < b.space
	}
	return bytes.Compare(a.key, b.key) < 0
}

func lockCompareByTx(a, b *lock) bool {
	if a.owner != b.owner {
		return a.owner < b.owner
	}
	return lockCompare(a, b)
}
