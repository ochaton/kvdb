package kvdb

import (
	"bytes"
	"errors"
	"reflect"

	"github.com/tidwall/btree"
)

type Space struct {
	name *string
	tree *btree.BTreeG[*record]
	wr   *writer
}

func newSpace(name string, wr *writer) Space {
	return Space{
		name: &name,
		tree: btree.NewBTreeG(func(a, b *record) bool {
			return bytes.Compare(a.Key, b.Key) < 0
		}),
		wr: wr,
	}
}

func (s *Space) View() Space {
	return Space{
		name: s.name,
		tree: s.tree.Copy(),
		wr:   nil,
	}
}

func (s *Space) Len() int {
	return s.tree.Len()
}

func (s *Space) GE(key []byte, iter func(value any) bool) {
	s.tree.Ascend(&record{Key: key}, func(r *record) bool {
		return iter(r.Value)
	})
}

func (s *Space) LE(key []byte, iter func(value any) bool) {
	s.tree.Descend(&record{Key: key}, func(r *record) bool {
		return iter(r.Value)
	})
}

func (s *Space) Min() any {
	if min, ok := s.tree.Min(); ok {
		return min.Value
	}
	return nil
}

func (s *Space) Max() any {
	if max, ok := s.tree.Max(); ok {
		return max.Value
	}
	return nil
}

func (s *Space) Get(key []byte, into any) error {
	if key == nil {
		return ErrKeyIsNil
	}
	if reflect.ValueOf(into).Kind() != reflect.Ptr {
		return ErrIntoIsNotPointer
	}

	if rec, found := s.treeGet(&record{Key: key}); found {
		return rec.into(into)
	}
	return ErrNotFound
}

func (s *Space) Set(key []byte, value any) error {
	if key == nil {
		return ErrKeyIsNil
	}

	rec := &record{
		LSN:   0, // it will be set after successful write
		Key:   key,
		Value: value,
		Tag:   *s.name,
	}
	if err := s.writeSet(rec); err != nil {
		return err
	}
	_, _ = s.treeSet(rec)

	return nil
}

func (s *Space) Del(key []byte) error {
	if key == nil {
		return ErrKeyIsNil
	}

	rec := &record{
		Key: key,
		Tag: *s.name,
	}
	if err := s.writeDel(rec); err != nil {
		return err
	}
	_, _ = s.treeDel(rec)

	return nil
}

func (s *Space) Iter() SpaceIterator {
	iter := s.tree.Iter()
	iter.First()
	return SpaceIterator{iter, false}
}

/******************************************************************************
 * inner disk operations
 */

// Writes a set operation to the writer.
func (s *Space) writeSet(record *record) error {
	op := newOperation(record, OPERATION_SET)
	if err := s.wr.Write(&op); err != nil {
		return err
	}
	op.upgradeRecord()
	return nil
}

// Writes a delete operation to the writer.
func (s *Space) writeDel(record *record) error {
	op := newOperation(record, OPERATION_DEL)
	if err := s.wr.Write(&op); err != nil {
		return err
	}
	op.upgradeRecord()
	return nil
}

/******************************************************************************
 * inner tree operations
 */

// Sets new record into the tree.
func (s *Space) treeSet(r *record) (prev *record, err error) {
	if r == nil {
		return nil, ErrRecordIsNil
	}

	prev, _ = s.tree.Set(r)
	return
}

// Deletes record from the tree.
func (s *Space) treeDel(r *record) (prev *record, err error) {
	if r == nil {
		return nil, ErrRecordIsNil
	}

	prev, _ = s.tree.Delete(r)
	return
}

// Gets record from the tree.
func (s *Space) treeGet(r *record) (rec *record, found bool) {
	if r == nil {
		return nil, false
	}
	rec, found = s.tree.Get(r)
	return
}

type SpaceIterator struct {
	iter     btree.IterG[*record]
	finished bool
}

func (sIt *SpaceIterator) HasNext() bool {
	return !sIt.finished
}

func (sIt *SpaceIterator) next() *record {
	if sIt.finished {
		return nil
	}

	record := sIt.iter.Item()
	if !sIt.iter.Next() || record == nil {
		sIt.finished = true
	}
	return record
}

func (sIt *SpaceIterator) Next(into any) error {
	if record := sIt.next(); record != nil {
		return record.into(into)
	}
	return errors.New("no next value")
}

func (sIt *SpaceIterator) collectNext(size int) []*record {
	records := make([]*record, 0, size)
	for len(records) < size {
		record := sIt.next()
		if record == nil {
			break
		}
		records = append(records, record)
	}
	return records
}

func (sIt *SpaceIterator) Release() {
	sIt.iter.Release()
}
