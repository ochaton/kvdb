package helpers

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/ochaton/kvdb"
)

const (
	DbPath = ".kvdb"
)

func SetupDB(path string, withClean bool) (*kvdb.T, error) {
	if withClean {
		if err := CleanDB(path); err != nil {
			return nil, err
		}
	}
	db, err := kvdb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %v", err)
	}
	return db, nil
}

func CleanDB(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("failed to clean db: %v", err)
	}
	return nil
}

type Action func() error

func RunInParallel(actions []Action) error {
	chs := make([]chan error, 0, len(actions))
	for range len(actions) {
		chs = append(chs, make(chan error, 1))
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(actions))

	for i, action := range actions {
		go func(errorChan chan error) {
			defer wg.Done()
			defer close(errorChan)
			errorChan <- action()
		}(chs[i])
	}

	wg.Wait()

	var err error
	for _, ch := range chs {
		if localErr, ok := <-ch; ok && localErr != nil {
			err = errors.Join(err, localErr)
		}
	}

	return err
}

type TestUser struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type UniqueDataGenerator struct {
	curr int
}

func (gen *UniqueDataGenerator) Create(count int) []TestUser {
	users := make([]TestUser, 0, count)
	for range count {
		gen.curr += 1
		users = append(users, TestUser{
			Name: fmt.Sprintf("Alice-%d", gen.curr),
			Age:  gen.curr,
		})
	}
	gen.curr++
	return users
}

func (gen *UniqueDataGenerator) Change(item *TestUser) {
	item.Age = gen.curr
	gen.curr++
}

func Compare(existed, expected TestUser) bool {
	return existed.Name == expected.Name && existed.Age == expected.Age
}

func Merge(arr1, arr2 []TestUser) []TestUser {
	common := make([]TestUser, 0, len(arr1)+len(arr2))
	common = append(common, arr1...)
	common = append(common, arr2...)
	return common
}
