# ARCH

UserRecord contains kvdb.Record
kvdb.Key is a key of every Record

db.Get(Key, RetValue) error
db.Put(UserRecord) error
db.Delete(Key) error
db.CaD(&OldRecord) error
db.CaS(&OldRecord, NewRecord) error

Namespace.Operation(UserRecord) -> error

UserRecord created like that:

```go
// First user is embed kvdb.Record into it's Record
type UserRecord struct {
    kvdb.Record
    Name string `ion:"name"`
}

kvdb.Register("User")

// TypeName has to be defined by user, to specify Tag
func (*UserRecord) TypeName() string {
    return "User"
}
```
