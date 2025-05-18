package kvdb

type messageAction uint

const (
	messageActionNone    messageAction = 0
	messageActionWrite   messageAction = 1
	messageActionRotate  messageAction = 2
	messageActionCompact messageAction = 3
)

type message interface {
	Action() messageAction
	Callback() chan error
	Op() *operation
}

type messageBase struct {
	callback chan error
}

func newMessage() messageBase {
	return messageBase{
		callback: make(chan error, 1),
	}
}

func (m messageBase) Callback() chan error {
	return m.callback
}

func (m messageBase) Action() messageAction {
	return messageActionNone
}

func (m messageBase) Op() *operation {
	return nil
}

type messageWrite struct {
	messageBase
	op *operation
}

func (m *messageWrite) Action() messageAction {
	return messageActionWrite
}

func (m *messageWrite) Op() *operation {
	return m.op
}

func newWriteMessage(op *operation) message {
	if op == nil {
		return nil
	}
	return &messageWrite{
		messageBase: newMessage(),
		op:          op,
	}
}

type messageRotate struct {
	messageBase
}

func (m *messageRotate) Action() messageAction {
	return messageActionRotate
}

func newRotateMessage() message {
	return &messageRotate{
		messageBase: newMessage(),
	}
}

type messageCompact struct {
	messageBase
	lsn  uint64
	snap map[string]space
}

func (m *messageCompact) Action() messageAction {
	return messageActionCompact
}

func (m *messageCompact) Snap() map[string]space {
	return m.snap
}

func (m *messageCompact) LSN() uint64 {
	return m.lsn
}

func newCompactMessage(snap map[string]space, lsn uint64) message {
	if snap == nil {
		return nil
	}
	return &messageCompact{
		messageBase: newMessage(),
		snap:        snap,
		lsn:         lsn,
	}
}
