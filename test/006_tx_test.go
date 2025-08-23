package main_test

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ochaton/kvdb"
	"github.com/ochaton/kvdb/test/helpers"
)

func TestTxRepeatableReads(t *testing.T) {
	/* test repeatable reads
	problem:
		description: transaction reads the same row twice and gets a different value each time
		example: tx1 reads data. Due to concurrency, tx2 updates the same data and commit.
				 now if tx1 rereads the same data, it will retrieve a different value.
	test:
		tx0: get [space1, key1] - old item
		tx1: set [space1, key1] - locks until tx1 is finished
		tx1: commit
		tx0: get [space1] - old item
	*/
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// setup data
	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	user := helpers.TestUser{Name: "Alice-1", Age: 1}
	if err := sp.Set(user.Key(), user); err != nil {
		t.Fatalf("failed to setup data: %v", err)
	}
	changedUser := helpers.TestUser{Name: user.Name, Age: user.Age + 1}

	// order channels
	orderCh := helpers.NewOrderChans(2)
	orderCh.Run(0)

	// run in parallel
	actions := []helpers.Action{
		func() error {
			orderCh.Wait(0)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx0 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				fResult, err := sp.Get(user.Key())
				if err != nil {
					return err
				}
				orderCh.Run(1)
				t.Logf("tx0 wait for tx1 commit or timeout after 1s\n")
				orderCh.WaitWithTimeout(0, 1*time.Second)
				t.Logf("tx0 stop waiting\n")

				sResult, err := sp.Get(user.Key())
				if err != nil {
					return err
				}

				if !reflect.DeepEqual(fResult, sResult) {
					return fmt.Errorf("failed do to repeatable reads: %v != %v", fResult, sResult)
				}

				t.Logf("tx0 commit\n")
				return nil
			})
			return err
		},
		func() error {
			orderCh.Wait(1)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx1 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")

				if err := sp.Set(user.Key(), changedUser); err != nil {
					return err
				}
				t.Logf("tx1 commit\n")
				return nil
			})
			orderCh.Run(0)
			return err
		},
	}

	if err = helpers.RunInParallel(actions); err != nil {
		t.Fatalf("failed to run actions in parallel: %v", err)
	}

	orderCh.Release()

	// check user after transactions
	resultUser, err := sp.Get(user.Key())
	if err != nil {
		t.Fatalf("failed to get user after transactions: %v", err)
	}
	if !reflect.DeepEqual(resultUser, changedUser) {
		t.Fatalf("failed check result user: %v != %v", resultUser, changedUser)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTxNonPhantomReads(t *testing.T) {
	/* test non phantom read
	problem:
		description: same queries return different resutl
		example: tx1 retrieves a set of rows. tx2 generate new rows.
				 now if tx1 retry select, it will retrieve a different values.
	test:
		tx0: list [space1, key1] - old items
		tx1: set [space1, key1] - locks until tx1 is finished
		tx1: commit
		tx0: list [space1] - old items
	*/
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// setup data
	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	oldUsers := factory.Generate(3)
	for _, user := range oldUsers {
		if err := sp.Set(user.Key(), user); err != nil {
			t.Fatalf("failed to setup data: %v", err)
		}
	}
	newUser := factory.One()
	newUsers := []helpers.TestUser{}
	newUsers = append(newUsers, oldUsers...)
	newUsers = append(newUsers, newUser)

	// order channels
	orderCh := helpers.NewOrderChans(2)
	orderCh.Run(0)

	// run in parallel
	actions := []helpers.Action{
		func() error {
			orderCh.Wait(0)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx0 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				fResults, err := sp.List().Collect()
				if err != nil {
					return err
				}
				orderCh.Run(1)
				t.Logf("tx0 wait for tx1 commit or timeout after 1s\n")
				orderCh.WaitWithTimeout(0, 1*time.Second)
				t.Logf("tx0 stop waiting\n")

				sResults, err := sp.List().Collect()
				if err != nil {
					return err
				}

				if !reflect.DeepEqual(fResults, sResults) {
					return fmt.Errorf("failed do to phantom reads: %v != %v", fResults, sResults)
				}

				t.Logf("tx0 commit\n")
				return nil
			})
			return err
		},
		func() error {
			orderCh.Wait(1)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx1 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				if err := sp.Set(newUser.Key(), newUser); err != nil {
					return err
				}
				t.Logf("tx1 commit\n")
				return nil
			})
			orderCh.Run(0)
			return err
		},
	}

	if err = helpers.RunInParallel(actions); err != nil {
		t.Fatalf("failed to run actions in parallel: %v", err)
	}

	orderCh.Release()

	finalResults, err := sp.List().Collect()
	if err != nil {
		t.Fatalf("failed to get result users: %v", err)
	}
	if !reflect.DeepEqual(finalResults, newUsers) {
		t.Fatalf("failed check result users: %v != %v", finalResults, newUsers)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTxNonDirtyReadsUpdate(t *testing.T) {
	/* test non dirty reads if parallel tx commit update
	problem:
		description: tx see uncommitted data of different tx
		example: tx1 update row, but not commit it.
		         in the same time tx2 read row and see uncommitted data
	test:
		- 1:
			tx1: update [space1, key1]
			tx2: get [space1,key1] - lock until commit of tx1. return updated item
			tx1: commit
			tx2: unlock get -> see new record
		- 2:
			tx1: update [space1, key1]
			tx2: get [space1,key1] - lock until commit/rollback of tx1
			tx1: rollback
			tx2: unlock get -> see old record
	*/
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// setup date
	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	users := factory.Generate(2)
	for _, user := range users {
		if err := sp.Set(user.Key(), user); err != nil {
			t.Fatalf("failed to setup data: %v", err)
		}
	}
	changedUsers := []helpers.TestUser{}
	for _, user := range users {
		changedUsers = append(changedUsers, helpers.TestUser{
			Name: user.Name,
			Age:  user.Age + 100,
		})
	}

	// order channels
	ordersCh := helpers.NewOrderChans(4)
	ordersCh.Run(0)
	ordersCh.Run(2)

	// run in parallel
	actions := []helpers.Action{
		func() error {
			ordersCh.Wait(0)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx0 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")

				if err := sp.Set(users[0].Key(), changedUsers[0]); err != nil {
					return err
				}
				ordersCh.Run(1)
				t.Logf("tx0 wait for tx1 commit or timeout after 1s\n")
				ordersCh.WaitWithTimeout(0, 1*time.Second)
				t.Logf("tx0 stop waiting\n")

				checkingUser, err := sp.Get(users[0].Key())
				if err != nil {
					return err
				}

				if !reflect.DeepEqual(checkingUser, changedUsers[0]) {
					return fmt.Errorf("failed check users: %v != %v", checkingUser, changedUsers[0])
				}

				t.Logf("tx0 commit\n")
				return nil
			})
			return err
		},
		func() error {
			ordersCh.Wait(1)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx1 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				checkingUser, err := sp.Get(users[0].Key())
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(checkingUser, changedUsers[0]) {
					return fmt.Errorf("failed to do undirty reads: %v != %v", checkingUser, changedUsers[0])
				}
				t.Logf("tx1 commit\n")
				return nil
			})
			ordersCh.Run(0)
			return err
		},
		func() error {
			ordersCh.Wait(2)
			rollbackErr := errors.New("rollback")
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx2 start\n")

				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				if err := sp.Set(users[1].Key(), changedUsers[1]); err != nil {
					return err
				}

				ordersCh.Run(3)
				t.Logf("tx2 wait for tx3 commit or timeout after 1s\n")
				ordersCh.WaitWithTimeout(2, 1*time.Second)
				t.Logf("tx2 stop waiting\n")

				checkingUser, err := sp.Get(users[1].Key())
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(checkingUser, changedUsers[1]) {
					return fmt.Errorf("failed check users: %v != %v", checkingUser, changedUsers[0])
				}

				t.Logf("tx2 rollback\n")
				return rollbackErr
			})
			if errors.Is(err, rollbackErr) {
				return nil
			}
			return err
		},
		func() error {
			ordersCh.Wait(3)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx3 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				checkingUser, err := sp.Get(users[1].Key())
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(checkingUser, users[1]) {
					return fmt.Errorf("failed to do undirty reads: %v != %v", checkingUser, users[1])
				}
				t.Logf("tx3 commit\n")
				return nil
			})
			ordersCh.Run(2)
			return err
		},
	}

	if err = helpers.RunInParallel(actions); err != nil {
		t.Fatalf("failed to run actions in parallel: %v", err)
	}

	ordersCh.Release()

	// check final result
	checkingUser1, err := sp.Get(users[0].Key())
	if err != nil {
		t.Fatalf("failed to get result user: %v", err)
	}
	checkingUser2, err := sp.Get(users[1].Key())
	if err != nil {
		t.Fatalf("failed to get result user: %v", err)
	}
	if !reflect.DeepEqual(checkingUser1, changedUsers[0]) {
		t.Fatalf(
			"failed to check result of tx0-tx1 (result of tx0 is commited): %v != %v",
			checkingUser1, changedUsers[0],
		)
	}
	if !reflect.DeepEqual(checkingUser2, users[1]) {
		t.Fatalf(
			"failed to check result of tx2-tx3 (result of tx2 is rollbacked): %v != %v",
			checkingUser2, users[1],
		)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTxNonDirtyReadsDelete(t *testing.T) {
	/* test non dirty reads if parallel tx commit update
	problem:
		description: tx see uncommitted data of different tx
		example: tx1 update row, but not commit it.
		         in the same time tx2 read row and see uncommitted data
	test:
		- 1:
			tx0: delete [space1, key1]
			tx1: get [space1,key1] - lock until commit of tx1. return updated item
			tx0: commit
			tx1: unlock get -> dont see record
		- 2:
			tx0: delete [space1, key1]
			tx1: get [space1,key1] - lock until commit/rollback of tx1
			tx0: rollback
			tx1: unlock get -> see old record
	*/
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// setup date
	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	users := factory.Generate(2)
	for _, user := range users {
		if err := sp.Set(user.Key(), user); err != nil {
			t.Fatalf("failed to setup data: %v", err)
		}
	}
	finalUsers := []helpers.TestUser{users[1]}

	// order channels
	ordersCh := helpers.NewOrderChans(4)
	ordersCh.Run(0)
	ordersCh.Run(2)

	// run in parallel
	actions := []helpers.Action{
		func() error {
			ordersCh.Wait(0)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx0 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")

				if err := sp.Del(users[0].Key()); err != nil {
					return err
				}
				ordersCh.Run(1)
				t.Logf("tx0 wait for tx1 commit or timeout after 1s\n")
				ordersCh.WaitWithTimeout(0, 1*time.Second)
				t.Logf("tx0 stop waiting\n")

				if _, err := sp.Get(users[0].Key()); err != kvdb.ErrNotFound {
					return fmt.Errorf("tx0: should see no user but get different error: %v", err)
				}

				t.Logf("tx0 commit\n")
				return nil
			})
			return err
		},
		func() error {
			ordersCh.Wait(1)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx1 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				if _, err := sp.Get(users[0].Key()); err != kvdb.ErrNotFound {
					return fmt.Errorf("tx1: should see no user but get different error: %v", err)
				}
				t.Logf("tx1 commit\n")
				return nil
			})
			ordersCh.Run(0)
			return err
		},
		func() error {
			ordersCh.Wait(2)
			rollbackErr := errors.New("rollback")
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx2 start\n")

				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				if err := sp.Del(users[1].Key()); err != nil {
					return err
				}

				ordersCh.Run(3)
				t.Logf("tx2 wait for tx3 commit or timeout after 1s\n")
				ordersCh.WaitWithTimeout(2, 1*time.Second)
				t.Logf("tx2 stop waiting\n")

				if _, err := sp.Get(users[1].Key()); err != kvdb.ErrNotFound {
					return fmt.Errorf("tx2: should see no user but get different error: %v", err)
				}

				t.Logf("tx2 rollback\n")
				return rollbackErr
			})
			if errors.Is(err, rollbackErr) {
				return nil
			}
			return err
		},
		func() error {
			ordersCh.Wait(3)
			err := db.Tx(func(exec kvdb.Executer) error {
				t.Logf("tx3 start\n")
				sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
				checkingUser, err := sp.Get(users[1].Key())
				if err != nil {
					return err
				}
				if !reflect.DeepEqual(checkingUser, users[1]) {
					return fmt.Errorf("tx3: failed to compare users: %v != %v", checkingUser, users[1])
				}
				t.Logf("tx3 commit\n")
				return nil
			})
			ordersCh.Run(2)
			return err
		},
	}

	if err = helpers.RunInParallel(actions); err != nil {
		t.Fatalf("failed to run actions in parallel: %v", err)
	}

	ordersCh.Release()

	// check final result
	scannedUsers, err := sp.List().Collect()
	if err != nil {
		t.Fatalf("failed to get results user: %v", err)
	}
	if !reflect.DeepEqual(finalUsers, scannedUsers) {
		t.Fatalf(
			"failed to check result: %v != %v",
			finalUsers, scannedUsers,
		)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTxList(t *testing.T) {
	/* Test list in tx. Should return result with correct data after
	- insert | update | delete
	*/
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// setup date
	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	users := factory.Generate(3)
	for _, user := range users {
		if err := sp.Set(user.Key(), user); err != nil {
			t.Fatalf("failed to setup data: %v", err)
		}
	}

	// changed data
	changedUser := helpers.TestUser{Name: users[1].Name, Age: users[1].Age + 100}
	newUser := factory.One()
	finalUsers := []helpers.TestUser{
		changedUser,
		newUser,
		users[2],
	}
	sort.Slice(finalUsers, func(i, j int) bool {
		return bytes.Compare(finalUsers[i].Key(), finalUsers[j].Key()) < 0
	})

	// run tx
	err = db.Tx(func(exec kvdb.Executer) error {
		t.Logf("tx1 start\n")
		sp := kvdb.GetSpace[helpers.TestUser](exec, "users")

		// delete
		if err := sp.Del(users[0].Key()); err != nil {
			return err
		}
		// update
		if err := sp.Set(users[1].Key(), changedUser); err != nil {
			return err
		}
		// insert
		if err := sp.Set(newUser.Key(), newUser); err != nil {
			return err
		}

		// list
		scannedUsers, err := sp.List().Collect()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(scannedUsers, finalUsers) {
			return fmt.Errorf("failed to compare users: %v != %v", scannedUsers, finalUsers)
		}
		t.Logf("tx1 commit\n")
		return nil
	})
	if err != nil {
		t.Fatalf("failed tx %v", err)
	}

	// final check
	scannedUsers, err := sp.List().Collect()
	if err != nil {
		t.Fatalf("failed to scan users %v", err)
	}
	if !reflect.DeepEqual(scannedUsers, finalUsers) {
		t.Fatalf("failed to final compare users: %v != %v", scannedUsers, finalUsers)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTxSet(t *testing.T) {
	/* test correct set in tx
	- set new record in tx (insert)
	- set old record in tx (update)
	- many sets for new record
	- many sets for old record
	*/
	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// setup date
	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	users := factory.Generate(2)
	for _, user := range users {
		if err := sp.Set(user.Key(), user); err != nil {
			t.Fatalf("failed to setup data: %v", err)
		}
	}
	usersCopy := helpers.CopyTestUsers(users)

	// run tx
	err = db.Tx(func(exec kvdb.Executer) error {
		t.Logf("tx1 start\n")
		sp := kvdb.GetSpace[helpers.TestUser](exec, "users")

		// set new record in tx (insert)
		newRecord1 := factory.One()
		usersCopy = append(usersCopy, newRecord1)
		if err := sp.Set(newRecord1.Key(), newRecord1); err != nil {
			return err
		}

		// set old record in tx (update)
		usersCopy[0].Age = usersCopy[0].Age + 100
		if err := sp.Set(usersCopy[0].Key(), usersCopy[0]); err != nil {
			return err
		}

		// many sets for new record
		newRecord2 := factory.One()
		for i := range 3 {
			newRecord2.Age = newRecord2.Age + i
			if err := sp.Set(newRecord2.Key(), newRecord2); err != nil {
				return err
			}
		}
		usersCopy = append(usersCopy, newRecord2)

		// many sets for old record
		for i := range 3 {
			usersCopy[1].Age = usersCopy[1].Age + i
			if err := sp.Set(usersCopy[1].Key(), usersCopy[1]); err != nil {
				return err
			}
		}

		// list
		scannedUsers, err := sp.List().Collect()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(scannedUsers, usersCopy) {
			return fmt.Errorf("failed to compare users: %v != %v", scannedUsers, usersCopy)
		}
		t.Logf("tx1 commit\n")
		return nil
	})
	if err != nil {
		t.Fatalf("failed tx %v", err)
	}

	// final check
	scannedUsers, err := sp.List().Collect()
	if err != nil {
		t.Fatalf("failed scan users %v", err)
	}
	if !reflect.DeepEqual(scannedUsers, usersCopy) {
		t.Fatalf("failed to final compare users: %v != %v", scannedUsers, usersCopy)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTxDel(t *testing.T) {
	/* test correct del in tx
	- del new record in tx - nothing
	- del old record in tx
	- many dels for new record
	- many dels for old record
	*/

	db, err := helpers.SetupDB(helpers.DbPath, true)
	if err != nil {
		t.Fatalf("%v", err)
	}

	// setup data
	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	factory := helpers.NewFactory(helpers.TestUser{}, helpers.NewTestUser)
	users := factory.Generate(4)
	for _, user := range users {
		if err := sp.Set(user.Key(), user); err != nil {
			t.Fatalf("failed to setup data: %v", err)
		}
	}

	// run tx
	err = db.Tx(func(exec kvdb.Executer) error {
		t.Logf("tx1 start\n")
		sp := kvdb.GetSpace[helpers.TestUser](exec, "users")
		// del new record in tx - nothing
		newRecord1 := factory.One()
		if err := sp.Del(newRecord1.Key()); err != nil {
			return err
		}
		// - del old record in tx
		if err := sp.Del(users[0].Key()); err != nil {
			return err
		}
		// - many dels for new record
		for range 3 {
			newRecord := factory.One()
			if err := sp.Del(newRecord.Key()); err != nil {
				return err
			}
		}
		// - many dels for old record
		for range 3 {
			if err := sp.Del(users[1].Key()); err != nil {
				return err
			}
		}

		// check
		scannedUsers, err := sp.List().Collect()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(scannedUsers, users[2:]) {
			return fmt.Errorf("failed to check users in tx: %v != %v", scannedUsers, users)
		}
		t.Logf("tx1 commit\n")
		return nil
	})
	if err != nil {
		t.Fatalf("failed tx %v", err)
	}

	// final check
	scannedUsers, err := sp.List().Collect()
	if err != nil {
		t.Fatalf("failed to scan users %v", err)
	}
	if !reflect.DeepEqual(scannedUsers, users[2:]) {
		t.Fatalf("failed to final check users in tx: %v != %v", scannedUsers, users)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}
