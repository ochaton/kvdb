package kvdb

import (
	"reflect"
	"testing"
)

const (
	spaceName = "test"
)

type mockWriter struct{}

func (mockWriter) Load(func(*operation) (uint64, error)) error { return nil }
func (mockWriter) Start() error                                { return nil }
func (mockWriter) Close() error                                { return nil }
func (mockWriter) Write(*operation) error                      { return nil }
func (mockWriter) Rotate() error                               { return nil }
func (mockWriter) Snapshot(*map[string]Space) error            { return nil }

type TestUser struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type TestBook struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func TestSpaceIterator(t *testing.T) {
	/* test success iterator run:
	- check full scan result
	*/
	space := newSpace(spaceName, mockWriter{})
	expectedData := []TestUser{
		{Name: "name-1", Age: 1},
		{Name: "name-2", Age: 2},
		{Name: "name-3", Age: 3},
	}
	for _, record := range expectedData {
		space.Set([]byte(record.Name), record)
	}

	scannedData := make([]TestUser, 0, len(expectedData))
	iter := space.Iter()
	defer iter.Release()
	for iter.HasNext() {
		ret := TestUser{}
		if err := iter.Next(&ret); err != nil {
			t.Fatalf("failed iter.Next with error: %v", err)
		}
		scannedData = append(scannedData, ret)
	}
	if !reflect.DeepEqual(scannedData, expectedData) {
		t.Fatalf("failed result check: loaded data: '%v', expected data: '%v'", scannedData, expectedData)
	}
}

func TestSpaceIteratorEmpty(t *testing.T) {
	/* test success iterator run for empty space */
	space := newSpace(spaceName, mockWriter{})

	iter := space.Iter()
	defer iter.Release()
	if iter.HasNext() {
		t.Fatalf("failed iter.HasNext(): result should be false because of empty space")
	}
}

func TestSpaceList(t *testing.T) {
	/* test success List
	- check full scan result
	*/
	space := newSpace(spaceName, mockWriter{})
	expectedData := []TestUser{
		{Name: "name-1", Age: 1},
		{Name: "name-2", Age: 2},
		{Name: "name-3", Age: 3},
	}
	for _, record := range expectedData {
		space.Set([]byte(record.Name), record)
	}

	scannedData := []TestUser{}
	if err := space.List(&scannedData); err != nil {
		t.Fatalf("failed space.List with error: %v", err)
	}
	if !reflect.DeepEqual(scannedData, expectedData) {
		t.Fatalf("failed result check: loaded data: '%v', expected data: '%v'", scannedData, expectedData)
	}
}

func TestSpaceListEmpty(t *testing.T) {
	/* test success space.List run for empty space */

	space := newSpace(spaceName, mockWriter{})
	scannedData := []TestUser{}

	if err := space.List(&scannedData); err != nil {
		t.Fatalf("failed space.List with error: %v", err)
	}
	if len(scannedData) != 0 {
		t.Fatalf("failed result check: loaded data '%v', expected data: '%v'", scannedData, []TestUser{})
	}
}

func TestSpaceListFailedNotSlicePointer(t *testing.T) {
	/* test error: pointer for into is not ponter to slice */
	space := newSpace(spaceName, mockWriter{})

	err := space.List(&TestBook{})
	if err != ErrIntoInvalidPointer {
		t.Fatalf(
			"failed scan.List with invalid error: have '%v', expected '%v'",
			err,
			ErrIntoInvalidPointer,
		)
	}
}

func TestSpaceListFailedIntoIsNotPonter(t *testing.T) {
	/* test error: pointer for into is not ponter*/
	space := newSpace(spaceName, mockWriter{})

	err := space.List([]TestBook{})
	if err != ErrIntoInvalidPointer {
		t.Fatalf(
			"failed scan.List with invalid error: have '%v', expected '%v'",
			err,
			ErrIntoInvalidPointer,
		)
	}
}

func TestSpaceListFailedInvalidType(t *testing.T) {
	/* test error: into has invalid type */
	// TODO: return after creating schema
	t.Skip("Skipping because we have no schema yet")
	space := newSpace(spaceName, mockWriter{})
	expectedData := []TestUser{
		{Name: "name-1", Age: 1},
		{Name: "name-2", Age: 2},
		{Name: "name-3", Age: 3},
	}
	for _, record := range expectedData {
		space.Set([]byte(record.Name), record)
	}

	err := space.List(&[]TestBook{})
	if err != ErrIntoInvalidType {
		t.Fatalf(
			"failed scan.List with invalid error: have '%v', expected '%v'",
			err,
			ErrIntoInvalidType,
		)
	}
}

func TestSpaceGet(t *testing.T) {
	/* test success get from space */
	space := newSpace(spaceName, mockWriter{})
	expectedData := []TestUser{
		{Name: "name-1", Age: 1},
		{Name: "name-2", Age: 2},
		{Name: "name-3", Age: 3},
	}
	for _, record := range expectedData {
		space.Set([]byte(record.Name), record)
	}

	ret := TestUser{}
	if err := space.Get([]byte(expectedData[0].Name), &ret); err != nil {
		t.Fatalf("failed scan.Get with error: %v", err)
	}
	if !reflect.DeepEqual(ret, expectedData[0]) {
		t.Fatalf("failed result check: loaded data: '%v', expected data: '%v'", ret, expectedData[0])
	}
}

func TestSpaceGetFailedKeyIsNil(t *testing.T) {
	/* test error: get from space with nil key */
	space := newSpace(spaceName, mockWriter{})

	err := space.Get(nil, &TestUser{})
	if err != ErrKeyIsNil {
		t.Fatalf("failed scan.Get with invalid error: have '%v', expected '%v'", err, ErrKeyIsNil)
	}
}

func TestSpaceGetFailedIntoIsNotPointer(t *testing.T) {
	/* test error: get from space with invalid into - not pointer */
	space := newSpace(spaceName, mockWriter{})

	err := space.Get([]byte("name-1"), TestUser{})
	if err != ErrIntoIsNotPointer {
		t.Fatalf("failed scan.Get invalid error: have '%v', expected '%v'", err, ErrIntoIsNotPointer)
	}
}

func TestSpaceGetFailedInvalidType(t *testing.T) {
	/* test error: get from space with invalid into type */
	// TODO: return after creating schema
	t.Skip("Skipping because we have no schema yet")
	space := newSpace(spaceName, mockWriter{})
	expectedData := []TestUser{
		{Name: "name-1", Age: 1},
		{Name: "name-2", Age: 2},
		{Name: "name-3", Age: 3},
	}
	for _, record := range expectedData {
		space.Set([]byte(record.Name), record)
	}

	err := space.Get([]byte(expectedData[0].Name), &TestBook{})
	if err != ErrIntoInvalidType {
		t.Fatalf("failed scan.Get invalid error: have '%v', expected '%v'", err, ErrIntoInvalidType)
	}
}

func TestSpaceGetFailedNotFound(t *testing.T) {
	/* test error: get from space not existed key */
	space := newSpace(spaceName, mockWriter{})
	expectedData := []TestUser{
		{Name: "name-1", Age: 1},
		{Name: "name-2", Age: 2},
		{Name: "name-3", Age: 3},
	}
	for _, record := range expectedData {
		space.Set([]byte(record.Name), record)
	}

	err := space.Get([]byte("name-4"), &TestBook{})
	if err != ErrNotFound {
		t.Fatalf("failed scan.Get invalid error: have '%v', expected '%v'", err, ErrNotFound)
	}
}
