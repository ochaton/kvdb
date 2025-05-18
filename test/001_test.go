package main_test

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"

	"github.com/ochaton/kvdb"
)

type TestUser struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestKVDB(t *testing.T) {
	db, err := kvdb.Open(".kvdb")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	users := db.NewSpace("users")
	alice := TestUser{Name: "Alice", Age: 30}

	if err := users.Set([]byte(alice.Name), alice); err != nil {
		t.Fatalf("failed to set user: %v", err)
	}

	var ret TestUser
	if err := users.Get([]byte(alice.Name), &ret); err != nil {
		t.Fatalf("failed to get user: %v", err)
	}

	if ret.Name != alice.Name || ret.Age != alice.Age {
		t.Fatalf("got %v, want %v", ret, alice)
	}

	// Check that mutation of ret does not affect the original
	ret.Age = 31
	if err := users.Get([]byte(alice.Name), &ret); err != nil {
		t.Fatalf("failed to get user: %v", err)
	}
	if ret.Age == 31 {
		t.Fatalf("got %v, want %v", ret.Age, alice.Age)
	}

	// check that any mutation of alice does not affect the original
	prevAliceAge := alice.Age
	alice.Age = 32
	if err := users.Get([]byte(alice.Name), &ret); err != nil {
		t.Fatalf("failed to get user: %v", err)
	}
	if ret.Age != prevAliceAge {
		t.Fatalf("got %v, want %v", ret.Age, prevAliceAge)
	}
}

func TestKVDBConcurrent(t *testing.T) {
	db, err := kvdb.Open(".kvdb")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	users := db.NewSpace("users")
	alice := TestUser{Name: "Alice", Age: 30}
	bob := TestUser{Name: "Bob", Age: 28}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	errs := make(chan error, 2)

	go func() {
		var err error
		defer wg.Done()
		defer func() {
			log.Printf("errs < err (1): %v", err)
			errs <- err
		}()

		if err = users.Set([]byte(alice.Name), alice); err != nil {
			err = fmt.Errorf("failed to set user: %v", err)
			return
		}
		var ret TestUser
		if err = users.Get([]byte(alice.Name), &ret); err != nil {
			err = fmt.Errorf("failed to get user: %v", err)
			return
		}
		if ret.Name != alice.Name || ret.Age != alice.Age {
			err = fmt.Errorf("got %v, want %v", ret, alice)
			return
		}
	}()

	go func() {
		var err error
		defer wg.Done()
		defer func() {
			log.Printf("errs < err (2): %v", err)
			errs <- err
		}()
		if err = users.Set([]byte(bob.Name), bob); err != nil {
			err = fmt.Errorf("failed to set user: %v", err)
			return
		}
		var ret TestUser
		if err = users.Get([]byte(bob.Name), &ret); err != nil {
			err = fmt.Errorf("failed to get user: %v", err)
			return
		}
		if ret.Name != bob.Name || ret.Age != bob.Age {
			err = fmt.Errorf("got %v, want %v", ret, bob)
			return
		}
	}()

	wg.Wait()
	limit := 2
	for limit > 0 {
		err := <-errs
		if err != nil {
			t.Error(err)
		}
		limit--
	}

	// Check that Bob and Alice are in the database
	var retAlice, retBob TestUser
	if err := users.Get([]byte(alice.Name), &retAlice); err != nil {
		t.Fatalf("failed to get user: %v", err)
	}
	if err := users.Get([]byte(bob.Name), &retBob); err != nil {
		t.Fatalf("failed to get user: %v", err)
	}
	if retAlice.Name != alice.Name || retAlice.Age != alice.Age {
		t.Fatalf("got %v, want %v", retAlice, alice)
	}
	if retBob.Name != bob.Name || retBob.Age != bob.Age {
		t.Fatalf("got %v, want %v", retBob, bob)
	}
}

type Metric struct {
	Name  []byte `json:"name"`
	Time  int64  `json:"time"`
	Value int64  `json:"value"`
}

func (m *Metric) Key() []byte {
	s := strconv.Itoa(int(m.Time))
	return append(m.Name, []byte(s)...)
}

func BenchmarkWrite(b *testing.B) {
	db, err := kvdb.Open(".kvdb")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}

	metrics := db.NewSpace("metrics")
	time := int64(0)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(p *testing.PB) {
		var m Metric
		k := []byte("metric")
		for p.Next() {
			time++
			m.Time = time
			m.Value = time
			m.Name = k
			if err := metrics.Set(m.Key(), m); err != nil {
				b.Fatalf("failed to set metric: %v", err)
			}
		}
	})
}
