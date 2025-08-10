package helpers

import (
	"fmt"
	"strconv"
)

type CreatorFn[T any] func(i int, key []byte) T
type ChangerFn[T any] func(item T, i int) T

/* test data */
type TestData[T any] interface {
	Key() []byte
}

type TestUser struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func NewTestUser(i int, nameKey []byte) TestUser {
	var name string
	if nameKey == nil {
		name = fmt.Sprintf("Alice-%d", i)
	} else {
		name = string(nameKey)
	}
	return TestUser{Name: name, Age: i}
}

func ChangeTestUser(user TestUser, i int) TestUser {
	user.Age = i
	return user
}

func (u *TestUser) Key() []byte {
	return []byte(u.Name)
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

func NewMetric(i int, _ []byte) Metric {
	time := int64(i)
	return Metric{Name: []byte("metric"), Time: time, Value: time}
}

/* test data factory */
type TestDataFactory[T any] struct {
	prototype T
	creator   CreatorFn[T]
	currCount int
}

func NewFactory[T any](
	prototype T,
	creator CreatorFn[T],
) *TestDataFactory[T] {
	return &TestDataFactory[T]{
		prototype: prototype,
		creator:   creator,
		currCount: 0,
	}
}

func (factory *TestDataFactory[T]) One() T {
	factory.currCount += 1
	return factory.creator(factory.currCount, nil)
}

func (factory *TestDataFactory[T]) OneWithKey(key []byte) T {
	factory.currCount += 1
	return factory.creator(factory.currCount, key)
}

func (factory *TestDataFactory[T]) Generate(count int) []T {
	data := make([]T, 0, count)
	for range count {
		factory.currCount += 1
		data = append(data, factory.creator(factory.currCount, nil))
	}
	return data
}

// func (factory TestDataFactory[T]) Change(item T) (T, error) {
// 	factory.currCount++
// 	return factory.changer(item, factory.currCount), nil
// }

func Merge[T any](arr1 []T, arr2 []T) []T {
	common := make([]T, 0, len(arr1)+len(arr2))
	common = append(common, arr1...)
	common = append(common, arr2...)
	return common
}

// type UniqueDataGenerator[T any] struct {
// 	curr int
// }

// func (gen *UniqueDataGenerator[T]) Create(count int) []T {
// 	users := make([]T, 0, count)
// 	for range count {
// 		gen.curr += 1
// 		users = append(users, TestUser{
// 			Name: fmt.Sprintf("Alice-%d", gen.curr),
// 			Age:  gen.curr,
// 		})
// 	}
// 	gen.curr++
// 	return users
// }

// func (gen *UniqueDataGenerator[T]) Change(item *TestUser) {
// 	item.Age = gen.curr
// 	gen.curr++
// }
