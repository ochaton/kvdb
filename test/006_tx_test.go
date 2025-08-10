package main_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ochaton/kvdb"
	"github.com/ochaton/kvdb/test/helpers"
)

func TestKVDBConcurrentTXWithSleeps(t *testing.T) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	usersTx1 := []helpers.TestUser{
		{Name: "Alice-1", Age: 1},
		{Name: "Alice-2", Age: 2},
	}
	usersTx2 := []helpers.TestUser{
		{Name: "Alice-1", Age: 4},
		{Name: "Alice-2", Age: 5},
		{Name: "Alice-3", Age: 6},
	}

	actions := []helpers.Action{}

	actions = append(actions, func() error {
		return db.Tx(func(exec kvdb.Executer) error {
			sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
			t.Logf("start TX1\n")
			for _, user := range usersTx1 {
				sp.Set(user.Key(), user)
			}
			time.Sleep(2 * time.Second)
			for _, user := range usersTx1 {
				ret, err := sp.Get(user.Key())
				if err != nil {
					return err
				}
				if ret != user {
					return fmt.Errorf("got %v, want %v", ret, user)
				}
			}
			t.Logf("finish TX1\n")
			return nil
		})
	})
	actions = append(actions, func() error {
		return db.Tx(func(exec kvdb.Executer) error {
			time.Sleep(1 * time.Second)
			t.Logf("start TX2\n")
			sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
			for _, user := range usersTx2 {
				sp.Set(user.Key(), user)
			}
			for _, user := range usersTx2 {
				ret, err := sp.Get(user.Key())
				if err != nil {
					return err
				}
				if ret != user {
					return fmt.Errorf("got %v, want %v", ret, user)
				}
			}
			t.Logf("finish TX2\n")
			return nil
		})
	})

	if err = helpers.RunInParallel(actions); err != nil {
		t.Fatalf("failed to run actions in parallel: %v", err)
	}

	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	scannedData, err := sp.List().Collect()
	if err != nil {
		t.Fatalf("failed to scan space users: %v", err)
	}
	if !reflect.DeepEqual(scannedData, usersTx2) {
		t.Fatalf("failed to load: loaded data: %v expected data: %v", scannedData, usersTx2)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestKVDBConcurrentTX(t *testing.T) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	usersTx1 := []helpers.TestUser{
		{Name: "Alice-1", Age: 1},
		{Name: "Alice-2", Age: 2},
	}
	usersTx2 := []helpers.TestUser{
		{Name: "Alice-1", Age: 4},
		{Name: "Alice-2", Age: 5},
		{Name: "Alice-3", Age: 6},
	}

	actions := []helpers.Action{}
	actions = append(actions, func() error {
		return db.Tx(func(exec kvdb.Executer) error {
			t.Logf("start TX1\n")
			sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
			for _, user := range usersTx1 {
				sp.Set(user.Key(), user)
			}
			for _, user := range usersTx1 {
				ret, err := sp.Get(user.Key())
				if err != nil {
					return err
				}
				if ret != user {
					return fmt.Errorf("got %v, want %v", ret, user)
				}
			}
			t.Logf("finish TX1\n")
			return nil
		})
	})
	actions = append(actions, func() error {
		return db.Tx(func(exec kvdb.Executer) error {
			t.Logf("start TX2\n")
			sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
			for _, user := range usersTx2 {
				sp.Set(user.Key(), user)
			}
			for _, user := range usersTx2 {
				ret, err := sp.Get(user.Key())
				if err != nil {
					return err
				}
				if ret != user {
					return fmt.Errorf("got %v, want %v", ret, user)
				}
			}
			t.Logf("finish TX2\n")
			return nil
		})
	})

	if err = helpers.RunInParallel(actions); err != nil {
		t.Fatalf("failed to run actions in parallel: %v", err)
	}

	// sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	// scannedData, err := sp.List().Collect()
	// if err != nil {
	// 	t.Fatalf("failed to scan space users: %v", err)
	// }
	// if !reflect.DeepEqual(scannedData, usersTx2) {
	// 	t.Fatalf("failed to load: loaded data: %v expected data: %v", scannedData, usersTx2)
	// }

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}
