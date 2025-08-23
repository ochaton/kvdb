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
	prerunning
	running
	stopped
	failed
)

type Writer interface {
	Load(applyFn processor.ApplyOperationFn) error
	Start() error
	Stop() error
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

func (w *DefaultWriter) Load(applyFn processor.ApplyOperationFn) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status != created {
		return ErrWriterInvalidStatus
	}

	if err := w.processor.Load(applyFn); err != nil {
		return err
	}
	w.status = loaded
	return nil
}

func (w *DefaultWriter) Start() error {
	if err := w.PreStart(); err != nil {
		return err
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go w.work(wg)
	wg.Wait()

	return nil
}

func (w *DefaultWriter) PreStart() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status != loaded {
		return ErrWriterIsNotLoaded
	}

	if err := w.processor.Rotate(); err != nil {
		return err
	}

	w.incoming = make(chan task, 100)
	w.status = prerunning
	return nil
}

func (w *DefaultWriter) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.status == stopped {
		return ErrWriterAlreadyStopped
	}

	w.status = stopped
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

func (w *DefaultWriter) work(wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			w.mu.Lock()
			w.status = failed
			w.mu.Unlock()
		}
	}()
	w.mu.Lock()
	w.done = make(chan error, 1)
	w.status = running
	w.mu.Unlock()
	wg.Done()

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
		return ErrWriterIsNotRunning
	}
	w.incoming <- t
	w.mu.RUnlock()
	return t.Wait()
}
