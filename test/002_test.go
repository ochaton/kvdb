package main_test

import (
	"fmt"
	"testing"

	"github.com/ochaton/kvdb/test/helpers"
)

func TestKVDBSnapshoting(t *testing.T) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}
	usersSpace, err := db.NewSpace("users")
	if err != nil {
		t.Fatalf("failed to create space users: %v", err)
	}

	// create data: 3 group
	dataGen := &helpers.UniqueDataGenerator{}
	usersBeforeSnap := dataGen.Create(3)
	usersAfterSnap := dataGen.Create(3)

	// do set + delete +replace operations
	for i, item := range usersBeforeSnap {
		if err = usersSpace.Set([]byte(item.Name), item); err != nil {
			t.Fatalf("failed to set user: %v", err)
		}
		if i%3 == 1 {
			if err = usersSpace.Del([]byte(item.Name)); err != nil {
				t.Fatalf("failed to del user: %v", err)
			}
		} else if i%3 == 2 {
			dataGen.Change(&item)
			usersBeforeSnap[i] = item
			if err = usersSpace.Set([]byte(item.Name), item); err != nil {
				t.Fatalf("failed to set existing user %v", err)
			}
		}
	}

	// do set operations and snapshoting in parallel + check errors
	actions := []helpers.Action{
		func() error {
			for i, item := range usersAfterSnap {
				if err = usersSpace.Set([]byte(item.Name), item); err != nil {
					return fmt.Errorf("failed to set user: %v", err)
				}
				if i%3 == 1 {
					if err = usersSpace.Del([]byte(item.Name)); err != nil {
						return fmt.Errorf("failed to del user: %v", err)
					}
				} else if i%3 == 2 {
					dataGen.Change(&item)
					usersAfterSnap[i] = item
					if err = usersSpace.Set([]byte(item.Name), item); err != nil {
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
	ret := helpers.TestUser{}
	for i, item := range helpers.Merge(usersBeforeSnap, usersAfterSnap) {
		if i%3 == 1 {
			if err := usersSpace.Get([]byte(item.Name), &ret); err == nil {
				t.Fatalf("get deleted user: %v", ret)
			}
		} else {
			if err := usersSpace.Get([]byte(item.Name), &ret); err != nil {
				t.Fatalf("failed to get user: %v", err)
			}
			if !helpers.Compare(item, ret) {
				t.Fatalf("got %v, want %v", ret, item)
			}
		}
	}
	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}
