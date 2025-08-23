package kvdb

import (
	"bytes"

	"github.com/google/uuid"
	"github.com/ochaton/kvdb/common/record"
	"github.com/ochaton/kvdb/locks"
	"github.com/tidwall/btree"
)

type txOpType string

const (
	TX_OPERATION_INSERT  txOpType = "insert"
	TX_OPERATION_REPLACE txOpType = "replace"
	TX_OPERATION_DELETE  txOpType = "delete"
)

type txOperation struct {
	op     txOpType
	record *record.Record
}

func newTxOperation(op txOpType, rec *record.Record) *txOperation {
	return &txOperation{
		op:     op,
		record: rec,
	}
}

func newTxOperationAsKey(space string, key []byte) *txOperation {
	return &txOperation{
		record: &record.Record{
			Space: space,
			Key:   key,
		},
	}
}

type TxExecter struct {
	ID             string
	defaultExec    *DB
	modified       *btree.BTreeG[*txOperation]
	itemsCountDiff int
	lockManager    locks.LockManager
}

func NewTXExecuter(db *DB) *TxExecter {
	return &TxExecter{
		ID:          uuid.New().String(),
		defaultExec: db,
		modified: btree.NewBTreeG(func(a, b *txOperation) bool {
			if a.record.Space != b.record.Space {
				return a.record.Space < b.record.Space
			}
			return bytes.Compare(a.record.Key, b.record.Key) < 0
		}),
		lockManager: db.lockManager,
	}
}

func (tx *TxExecter) isNewRecord(op *txOperation) (bool, error) {
	if oldTxOp, found := tx.modified.Get(op); found {
		return oldTxOp.op == TX_OPERATION_INSERT, nil
	}
	if _, err := tx.defaultExec.Get(op.record.Space, op.record.Key); err != nil {
		if err == ErrNotFound {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (tx *TxExecter) Set(spaceName string, key []byte, value any) error {
	tx.lockManager.LockRecord(tx.ID, spaceName, key)

	newTxOp := newTxOperation(TX_OPERATION_INSERT, record.NewRecord(spaceName, key, value))
	isNew, err := tx.isNewRecord(newTxOp)
	if err != nil {
		return err
	}
	if !isNew {
		newTxOp.op = TX_OPERATION_REPLACE
	} else {
		tx.itemsCountDiff++
	}
	tx.modified.Set(newTxOp)

	return nil
}

func (tx *TxExecter) Del(spaceName string, key []byte) error {
	tx.lockManager.LockRecord(tx.ID, spaceName, key)

	newTxOp := newTxOperation(TX_OPERATION_DELETE, record.NewRecord(spaceName, key, nil))
	isNew, err := tx.isNewRecord(newTxOp)
	if err != nil {
		return err
	}
	if isNew {
		tx.modified.Delete(newTxOp)
		tx.itemsCountDiff--
	} else {
		tx.modified.Set(newTxOp)
	}
	return nil
}

func (tx *TxExecter) Get(spaceName string, key []byte) (*record.Record, error) {
	tx.lockManager.LockRecord(tx.ID, spaceName, key)
	txOp, found := tx.modified.Get(newTxOperationAsKey(spaceName, key))
	if found {
		if txOp.op == TX_OPERATION_DELETE {
			return nil, ErrNotFound
		}
		return txOp.record, nil
	}
	return tx.defaultExec.Get(spaceName, key)
}

func (tx *TxExecter) Len(spaceName string) int {
	tx.lockManager.LockSpace(tx.ID, spaceName)

	return tx.defaultExec.Len(spaceName) + tx.itemsCountDiff
}

func (tx *TxExecter) Stream(spaceName string) (chan *record.Record, error) {
	tx.lockManager.LockSpace(tx.ID, spaceName)

	defaultCh, err := tx.defaultExec.Stream(spaceName)
	if err != nil {
		return nil, err
	}

	if tx.modified.Len() > 0 {
		mergeCh := make(chan *record.Record)
		go mergeChans(mergeCh, defaultCh, tx.modified)
		return mergeCh, nil
	}
	return defaultCh, nil
}

func (tx *TxExecter) Commit() error {
	toSet, toDel := []*record.Record{}, []*record.Record{}

	iter := tx.modified.Iter()
	iter.First()
	for {
		item := iter.Item()
		if item == nil {
			break
		}
		if item.op == TX_OPERATION_DELETE {
			toDel = append(toDel, item.record)
		} else {
			toSet = append(toSet, item.record)
		}
		if ok := iter.Next(); !ok {
			break
		}
	}
	iter.Release()
	tx.defaultExec.ModifyBatch(toDel, toSet)
	tx.lockManager.UnLockAll(tx.ID)
	return nil
}

func (tx *TxExecter) Rollback() error {
	tx.modified = nil
	tx.lockManager.UnLockAll(tx.ID)
	return nil
}

func mergeChans(
	output chan *record.Record,
	sourceCh chan *record.Record,
	modified *btree.BTreeG[*txOperation],
) {
	defer close(output)
	var txOp *txOperation
	var found bool
	for item := range sourceCh {
		txOp, found = modified.Get(newTxOperationAsKey(item.Space, item.Key))
		if !found {
			output <- item
			continue
		}
		if txOp.op == TX_OPERATION_REPLACE {
			output <- txOp.record
		}
	}
	iter := modified.Iter()
	defer iter.Release()
	isOk := iter.First()
	for isOk {
		txOp = iter.Item()
		if txOp == nil {
			break
		}
		if txOp.op == TX_OPERATION_INSERT {
			output <- txOp.record
		}
		isOk = iter.Next()
	}
}
