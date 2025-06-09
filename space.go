package kvdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btree"
)

// Internal structure for space
type space struct {
	name   *string
	tree   *btree.BTreeG[*record]
	wr     *writer
	resets *atomic.Uint64
}

func newSpace(name string, wr *writer) space {
	return space{
		name: &name,
		tree: btree.NewBTreeG(func(a, b *record) bool {
			return bytes.Compare(a.Key, b.Key) < 0
		}),
		wr:     wr,
		resets: new(atomic.Uint64),
	}
}

func (s *space) View() space {
	return space{
		name:   s.name,
		tree:   s.tree.Copy(),
		wr:     nil,
		resets: new(atomic.Uint64),
	}
}

// Return number of records in the space
func (s *space) Len() int {
	return s.tree.Len()
}

func (s *space) Alive() uint64 {
	return uint64(s.tree.Len())
}

func (s *space) Dead() uint64 {
	return s.resets.Load()
}

func (s *space) AlivePct() float64 {
	resets := float64(s.resets.Load())
	size := float64(s.tree.Len() + 1)
	return size / (resets + size)
}

// Sets new record into the tree.
func (s *space) TreeSet(r *record) (prev *record, err error) {
	if r == nil {
		return nil, errors.New("record is nil")
	}

	prev, _ = s.tree.Set(r)
	if prev != nil {
		s.Inc()
	}
	return
}

func (s *space) Inc() {
	s.resets.Add(1)
}

func (s *space) Dec(sub uint64) bool {
	for {
		now := s.resets.Load()
		if now < sub {
			return false
		}

		if s.resets.CompareAndSwap(now, now-sub) {
			return true
		}
	}
}

// Deletes record from the tree.
func (s *space) TreeDel(r *record) (prev *record, err error) {
	if r == nil {
		return nil, errors.New("record is nil")
	}

	prev, _ = s.tree.Delete(r)
	if prev != nil {
		s.Inc()
	}

	return
}

// Gets record from the space.
func (s *space) TreeGet(r *record) (rec *record, found bool) {
	if r == nil {
		return nil, false
	}
	rec, found = s.tree.Get(r)
	return
}

// Writes a set operation to the writer.
func (s *space) WriteSet(record *record) error {
	op := operation{
		Op:     set,
		Time:   time.Now().Unix(),
		Record: record,
	}

	if err := s.wr.Write(&op); err != nil {
		return err
	}

	record.LSN = op.LSN
	record.Time = op.Time
	return nil
}

// Writes a delete operation to the writer.
func (s *space) WriteDel(record *record) error {
	op := operation{
		Op:     del,
		Time:   time.Now().Unix(),
		Record: record,
	}
	if err := s.wr.Write(&op); err != nil {
		return err
	}
	record.LSN = op.LSN
	record.Time = op.Time
	return nil
}

// Checks if the operation is in the space with the same LSN.
func (s *space) HasLSN(op *operation) (bool, error) {
	if op == nil {
		return false, errors.New("operation is nil")
	}

	if op.Record.Tag != *s.name {
		return false, errors.New("operation tag does not match space tag")
	}

	rec, found := s.tree.Get(&record{
		Key: op.Record.Key,
	})

	if !found {
		return false, nil
	}

	if rec.LSN == op.LSN {
		return true, nil
	}

	return false, nil
}

type Header struct {
	LSN  uint64 `json:"lsn"`
	Time int64  `json:"time"` // unix timestamp
	Key  []byte `json:"key"`
}

// public interface for space
type Space interface {
	// Set {key, value} into the space (replacing the old value if exists)
	Set(key []byte, value any) error
	// Del {key} from the space
	Del(key []byte) error
	// Get {key} from the space
	// returns the record if found, otherwise returns error
	Get(key []byte, into any) error
	Len() int
	LE(key []byte, iter func(value any) bool)
	GE(key []byte, iter func(value any) bool)
	Min() (value any)
	Max() (value any)
}

// implementation of public interface of space
type impl struct {
	inner space
}

var _ Space = (*impl)(nil)

// common buffer pool
var bufPool = &sync.Pool{
	New: func() any {
		return bytes.NewBuffer(nil)
	},
}

func newImpl(sp space) *impl {
	return &impl{
		inner: sp,
	}
}

func (s *impl) Get(key []byte, into any) error {
	if key == nil {
		return errors.New("key is nil")
	}
	if reflect.ValueOf(into).Kind() != reflect.Ptr {
		return errors.New("into must be a pointer")
	}

	rec, found := s.inner.TreeGet(&record{
		Key: key,
	})

	if !found {
		return ErrNotFound
	}

	return s.copy(rec, into)
}

func (s *impl) copy(r *record, into any) error {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)

	buf.Reset()
	enc := json.NewEncoder(buf)
	dec := json.NewDecoder(buf)

	if err := enc.Encode(r.Value); err != nil {
		return err
	}
	if err := dec.Decode(into); err != nil {
		return err
	}

	// if into has field Header type of kvdb.Header
	v := reflect.ValueOf(into).Elem()
	num := v.NumField()

	for i := range num {
		if v.Type().Field(i).Type == reflect.TypeOf(Header{}) {
			v.Field(i).Set(reflect.ValueOf(Header{
				LSN:  r.LSN,
				Time: r.Time,
				Key:  r.Key,
			}))
			break
		}
	}

	return nil
}

func (s *impl) Len() int {
	return s.inner.tree.Len()
}

func (s *impl) GE(key []byte, iter func(value any) bool) {
	s.inner.tree.Ascend(&record{Key: key}, func(r *record) bool {
		return iter(r.Value)
	})
}

func (s *impl) LE(key []byte, iter func(value any) bool) {
	s.inner.tree.Descend(&record{Key: key}, func(r *record) bool {
		return iter(r.Value)
	})
}

func (s *impl) Min() any {
	min, ok := s.inner.tree.Min()
	if !ok {
		return nil
	} else {
		return min.Value
	}
}

func (s *impl) Max() any {
	max, ok := s.inner.tree.Max()
	if !ok {
		return nil
	} else {
		return max.Value
	}
}

func (s *impl) Set(key []byte, value any) error {
	rec := &record{
		LSN:   0, // it will be set after successful write
		Key:   key,
		Value: value,
		Tag:   *s.inner.name,
	}

	if err := s.inner.WriteSet(rec); err != nil {
		return err
	}

	_, _ = s.inner.TreeSet(rec)
	return nil
}

func (s *impl) Del(key []byte) error {
	rec := &record{
		Key: key,
		Tag: *s.inner.name,
	}

	if err := s.inner.WriteDel(rec); err != nil {
		return err
	}

	_, _ = s.inner.TreeDel(rec)
	return nil
}
