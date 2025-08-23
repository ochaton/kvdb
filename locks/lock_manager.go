package locks

import (
	"sync"
)

type LockManager interface {
	LockSpace(txID string, space string)
	LockRecord(txID string, space string, key []byte)
	UnLockAll(txID string)
}

type DefaultLockManager struct {
	storage *storage
	mu      sync.RWMutex
}

func NewLockManager() *DefaultLockManager {
	store := newStorage()
	return &DefaultLockManager{
		storage: store,
		mu:      sync.RWMutex{},
	}
}

func (lm *DefaultLockManager) LockSpace(txID string, space string) {
	for {
		if waitCh := lm.trySetSpaceLock(txID, space); waitCh != nil {
			<-waitCh
			continue
		}
		break
	}
}

func (lm *DefaultLockManager) trySetSpaceLock(txID string, space string) chan struct{} {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if spLock, spFound := lm.storage.GetSpaceLock(space); spFound {
		if spLock.owner != txID {
			return spLock.ch
		}
		return nil
	}
	if rLock, rFound := lm.storage.GetRecordLockInSpace(space); rFound && rLock.owner != txID {
		return rLock.ch
	}
	lm.storage.SetLock(newLock(txID, space, nil))
	return nil
}

func (lm *DefaultLockManager) LockRecord(txID string, space string, key []byte) {
	for {
		if waitCh := lm.trySetRecordLock(txID, space, key); waitCh != nil {
			<-waitCh
			continue
		}
		break
	}
}

func (lm *DefaultLockManager) trySetRecordLock(txID string, space string, key []byte) chan struct{} {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if spLock, spFound := lm.storage.GetSpaceLock(space); spFound {
		if spLock.owner != txID {
			return spLock.ch
		}
		return nil
	}
	if rLock, rFound := lm.storage.GetRecordLock(space, key); rFound {
		if rLock.owner != txID {
			return rLock.ch
		}
		return nil
	}
	lm.storage.SetLock(newLock(txID, space, key))
	return nil
}

func (lm *DefaultLockManager) UnLockAll(txID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.storage.DeleteLocksByTxID(txID)
}
