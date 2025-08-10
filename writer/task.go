package writer

import (
	"github.com/ochaton/kvdb/writer/operation"
	"github.com/ochaton/kvdb/writer/processor"
)

type task interface {
	Do(proc *processor.Processor)
	SendToCallback(err error)
	Wait() error
}

type taskBase struct {
	callback chan error
}

func newTaskBase() taskBase {
	return taskBase{
		callback: make(chan error, 1),
	}
}

func (t *taskBase) Do(proc *processor.Processor) {
	t.SendToCallback(nil)
}

func (t *taskBase) SendToCallback(err error) {
	if t.callback != nil {
		t.callback <- err
		close(t.callback)
	}
}

func (t *taskBase) Wait() error {
	return <-t.callback
}

// rotate task
type taskRotate struct {
	taskBase
}

func (t *taskRotate) Do(proc *processor.Processor) {
	t.SendToCallback(proc.Rotate())
}

func newRotateTask() task {
	return &taskRotate{
		taskBase: newTaskBase(),
	}
}

// snapshot task
type taskSnapshot struct {
	taskBase
	snap processor.Snapshot
}

func (t *taskSnapshot) Do(proc *processor.Processor) {
	if err := proc.Rotate(); err != nil {
		t.SendToCallback(err)
		return
	}
	lsn := proc.GetLSN()
	go func(currLSN uint64) {
		t.SendToCallback(proc.Snapshot(currLSN, t.snap))
	}(lsn)
}

func newSnapshotTask(snap processor.Snapshot) task {
	if snap == nil {
		return nil
	}
	return &taskSnapshot{
		taskBase: newTaskBase(),
		snap:     snap,
	}
}

// write task
type taskWrite struct {
	taskBase
	op *operation.Operation
}

func (t *taskWrite) Do(proc *processor.Processor) {
	t.SendToCallback(proc.Write(t.op))
}

func newWriteTask(op *operation.Operation) task {
	if op == nil {
		return nil
	}
	return &taskWrite{
		taskBase: newTaskBase(),
		op:       op,
	}
}

// write batch task
type taskWriteBatch struct {
	taskBase
	ops *operation.OperationBatch
}

func (t *taskWriteBatch) Do(proc *processor.Processor) {
	t.SendToCallback(proc.WriteBatch(t.ops))
}

func newWriteBatchTask(ops *operation.OperationBatch) task {
	if ops == nil {
		return nil
	}
	return &taskWriteBatch{
		taskBase: newTaskBase(),
		ops:      ops,
	}
}
