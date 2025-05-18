package kvdb

import "errors"

var ErrClosed = errors.New("kvdb is already closed")
var ErrNotFound = errors.New("record not found")
