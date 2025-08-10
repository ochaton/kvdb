package writer

import "errors"

var ErrWriterInvalidStatus = errors.New("writer invalid status for operation")
var ErrMessageInvalidType = errors.New("invalid message type")
var ErrUnexpectedNilOperation = errors.New("unexpected nil operation")
