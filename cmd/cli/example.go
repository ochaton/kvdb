package main

import (
	"fmt"
	"log"

	"github.com/ochaton/kvdb"
	"github.com/ochaton/kvdb/common/record"
)

type User struct {
	Header record.Header `json:"-"`
	Name   string        `json:"name"`
	Age    int           `json:"age"`
}

func (u *User) Key() []byte {
	return []byte(u.Name)
}

func main() {
	db, err := kvdb.Open(".kvdb")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	bob := User{Name: "Bob", Age: 28}

	// Guarantee that the space is created
	err = db.Tx(func(exec kvdb.Executer) (err error) {
		sp := kvdb.GetSpace[User](exec, "users")

		if err = sp.Set(bob.Key(), bob); err != nil {
			return
		}

		gettedBob, err := sp.Get(bob.Key())
		if err != nil {
			return
		}

		gettedBob.Age += 1
		if err = sp.Set(gettedBob.Key(), gettedBob); err != nil {
			return
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}

	sp := kvdb.GetSpace[User](db, "users")
	for range 10 {
		if err = sp.Set(bob.Key(), bob); err != nil {
			panic(err)
		}
		gettedBod, err := sp.Get(bob.Key())
		if err != nil {
			panic(err)
		}
		fmt.Printf("User: %s, Age: %d\n", gettedBod.Name, gettedBod.Age)
	}
}
