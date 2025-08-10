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

func SetupDB(path string, withClean bool) (*kvdb.DB, error) {
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

func SetupDBWithData(path string, data map[string]string) (*kvdb.DB, error) {
	if err := CleanDB(path); err != nil {
		return nil, err
	}
	if err := SetupDataFiles(path, data); err != nil {
		return nil, err
	}
	db, err := SetupDB(path, false)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func StopDBWithClean(db *kvdb.DB) error {
	if err := db.Close(); err != nil {
		return fmt.Errorf("failed to close db: %v", err)
	}
	return CleanDB(db.GetPath())
}

func CleanDB(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("failed to clean db: %v", err)
	}
	return nil
}

func GetSpaceData[T any](db *kvdb.DB, spaceName string) ([]T, error) {
	var scannedData []T
	err := db.View(func(exec kvdb.Executer) (err error) {
		sp := kvdb.GetSpace[T](exec, spaceName)
		scannedData, err = sp.List().Collect()
		return err
	})
	if err != nil {
		return []T{}, fmt.Errorf("failed to view: %v", err)
	}
	return scannedData, nil
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
