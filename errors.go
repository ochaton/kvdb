package kvdb

import "errors"

var ErrClosed = errors.New("kvdb is already closed")
var ErrNotFound = errors.New("record not found")
var ErrKeyIsNil = errors.New("key is nil")
var ErrIntoIsNotPointer = errors.New("into must be a pointer")
var ErrIntoInvalidPointer = errors.New("into must be a pointer to a slice")
var ErrIntoInvalidType = errors.New("into has invalid type")
var ErrIteratorNoNextValue = errors.New("iterator is finished: no next value")

// internalErrors
var ErrRecordIsNil = errors.New("record is nil")
var ErrOperationIsNil = errors.New("operation is nil")
var ErrOperationNotMatchSpace = errors.New("operation tag does not match space tag")
var ErrOperationUnknownType = errors.New("unknown operation type")
var ErrWriterInvalidStatus = errors.New("kvdb writer invalid status for operation")
var ErrMessageInvalidType = errors.New("invalid message type")
var ErrMessageCallbackIsNil = errors.New("message callback is nil")
