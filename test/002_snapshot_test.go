package main_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/ochaton/kvdb"
	"github.com/ochaton/kvdb/test/helpers"
)

func TestKVDBSnapshot(t *testing.T) {
	initialDataFiles := map[string]string{
		"0000000005.snap": `
{"lsn":1,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
`,
		"0000000006.jlog": `
{"lsn":6,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-4","value":{"name":"Alice-4","age":4}}}
{"lsn":7,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-5","value":{"name":"Alice-5","age":5}}}
{"lsn":8,"op":"del","time":1750280676,"record":{"space":"users","key":"Alice-5","value":null}}
{"lsn":9,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-6","value":{"name":"Alice-6","age":6}}}
{"lsn":10,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-6","value":{"name":"Alice-6","age":8}}}
`,
	}
	db, err := helpers.SetupDBWithData(helpers.DbPath, initialDataFiles)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if err := db.Snapshot(); err != nil {
		t.Fatalf("failed to snapshot: %v", err)
	}

	expectedFileData := map[string]string{
		"0000000010.snap": `
{"lsn":1,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
{"lsn":6,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-4","value":{"name":"Alice-4","age":4}}}
{"lsn":10,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-6","value":{"name":"Alice-6","age":8}}}
`,
		"0000000011.jlog": ``,
	}

	if err := helpers.CheckDataFiles(helpers.DbPath, expectedFileData); err != nil {
		t.Fatalf("failed to check data files: %v", err)
	}

	expectedData := []helpers.TestUser{
		{Name: "Alice-1", Age: 1},
		{Name: "Alice-3", Age: 8},
		{Name: "Alice-4", Age: 4},
		{Name: "Alice-6", Age: 8},
	}
	scannedData, err := helpers.GetSpaceData[helpers.TestUser](db, "users")
	if err != nil {
		t.Fatalf("failed to get space data: %v", err)
	}
	if !reflect.DeepEqual(scannedData, expectedData) {
		t.Fatalf("failed to compare: got '%v' expected '%v'", expectedData, scannedData)
	}

	if err := helpers.StopDBWithClean(db); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestKVDBSnapshotWithEmptyJlog(t *testing.T) {
	initialDataFiles := map[string]string{
		"0000000005.snap": `
{"lsn":1,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
`,
		"0000000006.jlog": ``,
	}

	db, err := helpers.SetupDBWithData(helpers.DbPath, initialDataFiles)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if err := db.Snapshot(); err != nil {
		t.Fatalf("failed to snapshot: %v", err)
	}

	if err := helpers.CheckDataFiles(helpers.DbPath, initialDataFiles); err != nil {
		t.Fatalf("failed to check data files: %v", err)
	}

	expectedData := []helpers.TestUser{
		{Name: "Alice-1", Age: 1},
		{Name: "Alice-3", Age: 8},
	}
	scannedData, err := helpers.GetSpaceData[helpers.TestUser](db, "users")
	if err != nil {
		t.Fatalf("failed to get space data: %v", err)
	}
	if !reflect.DeepEqual(scannedData, expectedData) {
		t.Fatalf("failed to compare: got '%v' expected '%v'", expectedData, scannedData)
	}

	if err := helpers.StopDBWithClean(db); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestKVDBSnapshotConcurrent(t *testing.T) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}
	sp := kvdb.GetSpace[helpers.TestUser](db, "users")

	// create data: 3 group
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	usersBeforeSnap := factory.Generate(3)
	usersAfterSnap := factory.Generate(3)

	// do set + delete +replace operations
	for i, user := range usersBeforeSnap {
		if err = sp.Set(user.Key(), user); err != nil {
			t.Fatalf("failed to set user: %v", err)
		}
		if i%3 == 1 {
			if err = sp.Del(user.Key()); err != nil {
				t.Fatalf("failed to del user: %v", err)
			}
		} else if i%3 == 2 {
			newUserWithSameKey := factory.OneWithKey(user.Key())
			usersBeforeSnap[i] = newUserWithSameKey
			if err = sp.Set(newUserWithSameKey.Key(), newUserWithSameKey); err != nil {
				t.Fatalf("failed to set existing user %v", err)
			}
		}
	}
	// do set operations and snapshoting in parallel + check errors
	actions := []helpers.Action{
		func() error {
			for i, user := range usersAfterSnap {
				if err = sp.Set(user.Key(), user); err != nil {
					return fmt.Errorf("failed to set user: %v", err)
				}
				if i%3 == 1 {
					if err = sp.Del(user.Key()); err != nil {
						return fmt.Errorf("failed to del user: %v", err)
					}
				} else if i%3 == 2 {
					newUserWithSameKey := factory.OneWithKey(user.Key())
					usersAfterSnap[i] = newUserWithSameKey
					if err = sp.Set(newUserWithSameKey.Key(), newUserWithSameKey); err != nil {
						return fmt.Errorf("failed to set existing user %v", err)
					}
				}
			}
			return nil
		},
		func() error {
			return db.Snapshot()
		},
	}
	if err = helpers.RunInParallel(actions); err != nil {
		t.Fatalf("failed to compact or set during compaction: %v", err)
	}

	// check data in db
	for i, user := range helpers.Merge(usersBeforeSnap, usersAfterSnap) {
		if i%3 == 1 {
			ret, err := sp.Get(user.Key())
			if err == nil {
				t.Fatalf("get deleted user: %v", ret)
			}
		} else {
			ret, err := sp.Get(user.Key())
			if err != nil {
				t.Fatalf("failed to get user: %v", err)
			}
			if !reflect.DeepEqual(ret, user) {
				t.Fatalf("got %v, want %v", ret, user)
			}
		}
	}
	if err := helpers.StopDBWithClean(db); err != nil {
		t.Fatalf("%v", err)
	}
}
