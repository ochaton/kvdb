package locks

import (
	"github.com/tidwall/btree"
)

type storage struct {
	byLock *btree.BTreeG[*lock]
	byTx   *btree.BTreeG[*lock]
}

func newStorage() *storage {
	return &storage{
		byLock: btree.NewBTreeG(lockCompare),
		byTx:   btree.NewBTreeG(lockCompareByTx),
	}
}

func (s *storage) SetLock(lock *lock) {
	//fmt.Printf("[%s][%s][%v] set lock %v\n", lock.owner, lock.space, lock.key, lock.ch)
	s.byLock.Set(lock)
	s.byTx.Set(lock)
}

func (s *storage) GetSpaceLock(space string) (*lock, bool) {
	if spLock, found := s.byLock.Get(&lock{space: space}); found {
		return spLock, found
	}
	return nil, false
}

func (s *storage) GetRecordLockInSpace(space string) (*lock, bool) {
	if s.byLock.Len() == 0 {
		return nil, false
	}
	iter := s.byLock.Iter()
	defer iter.Release()
	iter.First()
	iter.Seek(&lock{space: space})
	rLock := iter.Item()
	if rLock.space == space {
		return rLock, true
	}
	return nil, false
}

func (s *storage) GetRecordLock(space string, key []byte) (*lock, bool) {
	return s.byLock.Get(&lock{space: space, key: key})
}

func (s *storage) DeleteLocksByTxID(txID string) {
	locks := s.collectLocksByTxID(txID)
	for _, lock := range locks {
		s.byLock.Delete(lock)
		s.byTx.Delete(lock)
		lock.Release()
		//fmt.Printf("[%s][%s][%v] delete lock %v\n", lock.owner, lock.space, lock.key, lock.ch)
	}
}

func (s *storage) collectLocksByTxID(txID string) []*lock {
	locks := make([]*lock, 0)
	iter := s.byTx.Iter()
	defer iter.Release()
	iter.First()
	iter.Seek(&lock{owner: txID})
	for {
		lock := iter.Item()
		if lock == nil || lock.owner != txID {
			break
		}
		locks = append(locks, lock)
		if !iter.Next() {
			break
		}
	}

	return locks
}
