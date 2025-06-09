package main

import (
	"fmt"
	"log"

	"github.com/ochaton/kvdb"
)

type User struct {
	Header kvdb.Header `json:"-"`
	Name   string      `json:"name"`
	Age    int         `json:"age"`
}

func (u User) Key() []byte {
	return []byte(u.Name)
}

func main() {
	db, err := kvdb.Open(".kvdb")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	users := db.NewSpace("users")
	bob := User{Name: "Bob", Age: 28}

	// Guarantee that the space is created
	err = db.Update(func(space kvdb.GetSpace) (err error) {
		users := space("users")

		if err = users.Set(bob.Key(), bob); err != nil {
			return
		}

		var bob User
		if err = users.Get([]byte("Bob"), &bob); err != nil {
			return
		}

		bob.Age += 1
		if err = users.Set(bob.Key(), bob); err != nil {
			return
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}

	for range 10 {
		if err = users.Set(bob.Key(), bob); err != nil {
			panic(err)
		}
		var ret User
		err := users.Get(bob.Key(), &ret)
		if err != nil {
			panic(err)
		}
		fmt.Printf("User: %s, Age: %d\n", ret.Name, ret.Age)
	}
}
