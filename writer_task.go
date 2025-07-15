package kvdb

type taskAction uint

const (
	taskActionNone taskAction = iota
	taskActionWrite
	taskActionRotate
	taskActionSnapshot
)

type task interface {
	Action() taskAction
	SendToCallback(err error)
	Op() *operation
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

func (t taskBase) SendToCallback(err error) {
	if t.callback != nil {
		t.callback <- err
		close(t.callback)
	}
}

func (t taskBase) Action() taskAction {
	return taskActionNone
}

func (t taskBase) Op() *operation {
	return nil
}

func (t taskBase) Wait() error {
	return <-t.callback
}

type taskWrite struct {
	taskBase
	op *operation
}

func (t *taskWrite) Action() taskAction {
	return taskActionWrite
}

func (m *taskWrite) Op() *operation {
	return m.op
}

func newWriteTask(op *operation) task {
	if op == nil {
		return nil
	}
	return &taskWrite{
		taskBase: newTaskBase(),
		op:       op,
	}
}

type taskRotate struct {
	taskBase
}

func (t *taskRotate) Action() taskAction {
	return taskActionRotate
}

func newRotateTask() task {
	return &taskRotate{
		taskBase: newTaskBase(),
	}
}

type taskSnapshot struct {
	taskBase
	snap *map[string]Space
}

func (t *taskSnapshot) Action() taskAction {
	return taskActionSnapshot
}

func (t *taskSnapshot) Snap() *map[string]Space {
	return t.snap
}

func newSnapshotTask(snap *map[string]Space) task {
	if snap == nil {
		return nil
	}
	return &taskSnapshot{
		taskBase: newTaskBase(),
		snap:     snap,
	}
}
