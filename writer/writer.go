package writer

import (
	"errors"
	"sync"

	"github.com/ochaton/kvdb/writer/operation"
	"github.com/ochaton/kvdb/writer/processor"
)

type status int

const (
	created status = iota
	loaded
	running
	closed
)

type Writer interface {
	Start() error
	Close() error
	Load(applyTxn func(txn *operation.Operation) (uint64, error)) error
	Rotate() error
	Snapshot(snap processor.Snapshot) error
	Write(op *operation.Operation) error
	WriteBatch(ops *operation.OperationBatch) error
}

type DefaultWriter struct {
	status    status
	processor *processor.Processor
	incoming  chan task
	done      chan error
	mu        sync.RWMutex // guards channel
}

func NewWriter(path string) *DefaultWriter {
	return &DefaultWriter{
		status:    created,
		processor: processor.NewProcessor(path),
		mu:        sync.RWMutex{},
	}
}

func (w *DefaultWriter) Load(applyTxn func(txn *operation.Operation) (uint64, error)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status != created {
		return ErrWriterInvalidStatus
	}

	if err := w.processor.Load(applyTxn); err != nil {
		return err
	}
	w.status = loaded
	return nil
}

func (w *DefaultWriter) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status != loaded {
		return ErrWriterInvalidStatus
	}

	if err := w.processor.Rotate(); err != nil {
		return err
	}

	w.incoming = make(chan task, 100)
	w.status = running

	go w.work()
	return nil
}

func (w *DefaultWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status == closed {
		return ErrWriterInvalidStatus
	}

	w.status = closed
	if w.incoming != nil {
		close(w.incoming)
	}
	if w.done != nil {
		// await for all messages to be processed
		return <-w.done
	}
	return nil
}

func (w *DefaultWriter) Rotate() error {
	return w.send(newRotateTask())
}

func (w *DefaultWriter) Snapshot(snap processor.Snapshot) error {
	return w.send(newSnapshotTask(snap))
}

func (w *DefaultWriter) Write(op *operation.Operation) error {
	return w.send(newWriteTask(op))
}

func (w *DefaultWriter) WriteBatch(ops *operation.OperationBatch) error {
	return w.send(newWriteBatchTask(ops))
}

func (w *DefaultWriter) work() {
	w.mu.Lock()
	if w.status != running {
		w.mu.Unlock()
		return
	}
	w.done = make(chan error, 1)
	w.mu.Unlock()

	for task := range w.incoming {
		task.Do(w.processor)
	}

	w.done <- w.processor.Close()
	close(w.done)
}

func (w *DefaultWriter) send(t task) error {
	if t == nil {
		return errors.New("writer get empty task")
	}
	w.mu.RLock()
	if w.status != running {
		w.mu.RUnlock()
		return errors.New("writer worker is not running")
	}
	w.incoming <- t
	w.mu.RUnlock()
	return t.Wait()
}
