package kvdb

import "github.com/ochaton/kvdb/common/record"

type FilterFunc func(rec *record.Record) bool

type Dataframe[T any] struct {
	space   string
	stream  chan *record.Record
	data    []T
	filters []FilterFunc
	err     error
}

func newDataframe[T any](spaceName string) *Dataframe[T] {
	return &Dataframe[T]{space: spaceName}
}

func (df *Dataframe[T]) SetStream(stream chan *record.Record) {
	df.stream = stream
}

func (df *Dataframe[T]) SetError(err error) {
	df.err = err
}

func (df *Dataframe[T]) Filter(fn FilterFunc) *Dataframe[T] {
	df.filters = append(df.filters, fn)
	return df
}

/*
TODO: think about input in collect -> get writer where i should write
*/
func (df *Dataframe[T]) Collect() ([]T, error) {
	if df.err != nil {
		return df.data, df.err
	}
	var keep bool
	for rec := range df.stream {
		keep = true
		for _, filterFn := range df.filters {
			if !filterFn(rec) {
				keep = false
				break
			}
		}
		if keep {
			var item T
			if err := rec.Into(&item); err != nil {
				return []T{}, err
			}
			df.data = append(df.data, item)
		}
	}
	return df.data, nil
}

func (df *Dataframe[T]) Data() []T {
	return df.data
}
