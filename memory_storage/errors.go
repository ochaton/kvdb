package memory_storage

import "errors"

var ErrRecordIsNil = errors.New("record is nil")
var ErrRecordNotFound = errors.New("record not found")
var ErrSpaceNotFound = errors.New("space not found")
var ErrMemoryStorageStopped = errors.New("memory storage is stopped")
