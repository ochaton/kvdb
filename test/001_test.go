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
