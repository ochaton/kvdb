package main_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/ochaton/kvdb"
	"github.com/ochaton/kvdb/test/helpers"
)

func TestKVDBConcurrent(t *testing.T) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	expectedData := factory.Generate(2)
	scannedData := make([]helpers.TestUser, 0)

	actions := []helpers.Action{}
	for _, user := range expectedData {
		actions = append(actions, func() error {
			if err = sp.Set(user.Key(), user); err != nil {
				return fmt.Errorf("failed to set user: %v", err)
			}
			ret, err := sp.Get(user.Key())
			if err != nil {
				return fmt.Errorf("failed to get user: %v", err)
			}
			if !reflect.DeepEqual(ret, user) {
				return fmt.Errorf("got %v, want %v", ret, user)
			}
			return nil
		})
	}

	if err = helpers.RunInParallel(actions); err != nil {
		t.Fatalf("failed to run actions in parallel: %v", err)
	}

	for _, expUser := range expectedData {
		scanUser, err := sp.Get(expUser.Key())
		if err != nil {
			t.Fatalf("failed to get user: %v", err)
		}
		scannedData = append(scannedData, scanUser)
	}
	if !reflect.DeepEqual(scannedData, expectedData) {
		t.Fatalf("failed to load: loaded data: %v expected data: %v", scannedData, expectedData)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}
