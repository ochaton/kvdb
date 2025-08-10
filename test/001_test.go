package main_test

import (
	"reflect"
	"testing"

	"github.com/ochaton/kvdb"
	"github.com/ochaton/kvdb/test/helpers"
)

func TestKVDB(t *testing.T) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	user := factory.One()

	if err := sp.Set(user.Key(), user); err != nil {
		t.Fatalf("failed to set user: %v", err)
	}
	ret, err := sp.Get(user.Key())
	if err != nil {
		t.Fatalf("failed to get user: %v", err)
	}
	if !reflect.DeepEqual(ret, user) {
		t.Fatalf("failed to compare: got %v, want %v", ret, user)
	}

	if err := helpers.StopDBWithClean(db); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestKVDBMulti(t *testing.T) {
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	expectedData := factory.Generate(3)
	scannedData := make([]helpers.TestUser, 0)

	err = db.Update(func(exec kvdb.Executer) error {
		sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
		for _, user := range expectedData {
			if err := sp.Set(user.Key(), user); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}
	err = db.View(func(exec kvdb.Executer) error {
		sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
		for _, user := range expectedData {
			ret, err := sp.Get(user.Key())
			if err != nil {
				return err
			}
			scannedData = append(scannedData, ret)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to view: %v", err)
	}
	if !reflect.DeepEqual(scannedData, expectedData) {
		t.Fatalf("failed to compare: got '%v' expected '%v'", scannedData, expectedData)
	}

	if err := helpers.StopDBWithClean(db); err != nil {
		t.Fatalf("%v", err)
	}
}
