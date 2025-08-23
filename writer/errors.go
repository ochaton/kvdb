package writer

import "errors"

var ErrWriterAlreadyStopped = errors.New("writer is already stopped")
var ErrWriterIsNotRunning = errors.New("writer is not running")
var ErrWriterIsNotLoaded = errors.New("writer is not loaded")

var ErrWriterInvalidStatus = errors.New("writer invalid status for operation")
var ErrMessageInvalidType = errors.New("invalid message type")
var ErrUnexpectedNilOperation = errors.New("unexpected nil operation")
