package locks

import (
	"bytes"
	"sync"

	"github.com/tidwall/btree"
)

type Lock struct {
	space string
	key   []byte
	owner string
	ch    chan struct{}
	mu    sync.Mutex
}

func NewLock(owner, space string, key []byte) *Lock {
	return &Lock{space: space, key: key, owner: owner, ch: make(chan struct{}), mu: sync.Mutex{}}
}

func (l *Lock) IsSpaceLock() bool {
	return l.key == nil
}

type LockManager struct {
	tree    Tree
	txLocks map[string][]*Lock
	mu      sync.RWMutex
}

func NewLockManager() *LockManager {
	tree := NewTree()
	txLocks := make(map[string][]*Lock)
	return &LockManager{
		tree:    tree,
		txLocks: txLocks,
		mu:      sync.RWMutex{},
	}
}

func (lm *LockManager) LockSpace(txID string, space string) {
	for {
		lm.mu.Lock()
		lock, found := lm.tree.GetSpaceLock(space)
		if found && lock.owner != txID {
			lm.mu.Unlock()
			<-lock.ch
			continue
		}

		if !found || !lock.IsSpaceLock() {
			lock := NewLock(txID, space, nil)
			lm.tree.SetLock(lock)
			lm.addLockToTx(lock)
			lock.mu.Lock()
		}

		lm.mu.Unlock()
		return
	}
}

func (lm *LockManager) LockItem(txID string, space string, key []byte) {
	//fmt.Printf("[tx=%s] Lock item start\n", txID)
	for {
		//fmt.Printf("[tx=%s][attempt][space=%s][key=%s] Lock item start\n", txID, space, string(key))
		lm.mu.Lock()
		lock, found := lm.tree.GetItemLock(space, key)
		if found && lock.owner != txID {
			lm.mu.Unlock()
			//fmt.Printf("[tx=%s][attempt][space=%s][key=%s] Lock item wait\n", txID, space, string(key))
			<-lock.ch
			//fmt.Printf("[tx=%s][attempt][space=%s][key=%s] Lock item wait finish\n", txID, space, string(key))
			continue
		}

		if !found {
			lock := NewLock(txID, space, key)
			lm.tree.SetLock(lock)
			lm.addLockToTx(lock)
			lock.mu.Lock()
			//fmt.Printf("[tx=%s][attempt][space=%s][key=%s] Lock item finish - new lock %v\n", txID, space, string(key), lock.mu)
		} else {
			//fmt.Printf("[tx=%s][attempt][space=%s][key=%s] Lock item finish - old lock %v\n", txID, space, string(key), lock.mu)

		}
		lm.mu.Unlock()
		break
	}
	// fmt.Printf("[tx=%s] Lock item finish\n", txID)
	// if locks, ok := lm.txLocks[txID]; ok {
	// 	for _, lock := range locks {
	// 		fmt.Printf("--- [tx=%s] Lock item finish %v\n", txID, lock.mu)
	// 	}

	// }

}

func (lm *LockManager) UnLockAll(txID string) {
	//fmt.Printf("[tx=%s] unlock start-1\n", txID)

	lm.mu.Lock()
	defer lm.mu.Unlock()
	//fmt.Printf("[tx=%s] unlock start-2\n", txID)
	locks, ok := lm.txLocks[txID]
	if !ok {
		return
	}
	for _, lock := range locks {
		//fmt.Printf("[tx=%s][space=%s][key=%s] start unlock lock %v\n", txID, lock.space, string(lock.key), lock.mu)
		lock.mu.Unlock()
		lm.tree.DeleteLock(lock)
		close(lock.ch)
		//fmt.Printf("[tx=%s] broadcast lock\n", txID)
	}
	delete(lm.txLocks, txID)
	//fmt.Printf("[tx=%s] unlock finish\n", txID)
}

func (lm *LockManager) addLockToTx(lock *Lock) {
	if data, ok := lm.txLocks[lock.owner]; ok {
		data = append(data, lock)
		lm.txLocks[lock.owner] = data
	} else {
		lm.txLocks[lock.owner] = []*Lock{lock}
	}
}

type Tree struct {
	inner *btree.BTreeG[*Lock]
}

func NewTree() Tree {
	inner := btree.NewBTreeG(func(a, b *Lock) bool {
		if a.space != b.space {
			return a.space < b.space
		}
		return bytes.Compare(a.key, b.key) < 0
	})
	return Tree{inner: inner}
}

func (t *Tree) SetLock(lock *Lock) {
	//fmt.Printf("Set Lock = %v\n", lock)
	t.inner.Set(lock)
}

func (t *Tree) DeleteLock(lock *Lock) {
	//fmt.Printf("Delete Lock = %v\n", lock)
	t.inner.Delete(lock)
}

func (t *Tree) GetSpaceLock(space string) (*Lock, bool) {
	if t.inner.Len() == 0 {
		return nil, false
	}
	spLock, found := t.inner.Get(&Lock{space: space})
	if found {
		return spLock, found
	}
	iter := t.inner.Iter()
	iter.First()
	iter.Seek(&Lock{space: space})
	itemLock := iter.Item()
	iter.Release()

	if itemLock.space == space {
		return itemLock, true
	}
	return nil, false
}

func (t *Tree) GetItemLock(space string, key []byte) (*Lock, bool) {
	if t.inner.Len() == 0 {
		return nil, false
	}
	itemLock, found := t.inner.Get(&Lock{space: space, key: key})
	if found {
		return itemLock, found
	}
	return t.inner.Get(&Lock{space: space})
}

/*
lock space --> create spaceLock sL.mu.Lock()
lock row   --> row


row --> lock (for space)
space --> lock (for all rows)

getSpace(space) --> mutex
getRow(space key) --> mutex

lock space == [can not lock rows]
lock of row == [can not lock space]

lock space "A"
lock row ["A", 'a'] // blocked

lock row ["B", 'b']
lock space "B" // blocked

lock row ["C", 'c']
lock row ["C", 'c'] // blocked



actions:
	- set row
		- exclusiveLock[KEY]
		- other transactions can not read this row:
	- del row:
		- exclusiveLock[KEY]
		- other transactions stil can read this row:
	- get row:
		- sharedLock[KEY]
	- getForUpdate row:
		- exclusiveLock[KEY]
	- list rows:
		- sharedLock[SPACE]



tx1: --get[key=1]--           --set[key=2]--
tx2:                 --get[key=2]-- --set[key=2]--
sL:  ------------------------------
eL:                           ------------------------------



- see uncommited changes (dirty read)
- you see commited changes (non-repeatable read)
- you see new inserted rows during transaction (phantom reads)

Reads → S(space)
Writes → X(space)
*/
