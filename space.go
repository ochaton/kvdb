package kvdb

type Space[T any] struct {
	name string
	exec Executer
}

func newSpace[T any](name string, exec Executer) *Space[T] {
	return &Space[T]{
		name: name,
		exec: exec,
	}
}

func (s *Space[T]) Len() int {
	return s.exec.Len(s.name)
}

func (s *Space[T]) Get(key []byte) (T, error) {
	var item T
	if key == nil {
		return item, ErrKeyIsNil
	}

	rec, found := s.exec.Get(s.name, key)
	if !found {
		return item, ErrNotFound
	}
	if err := rec.Into(&item); err != nil {
		return item, err
	}
	return item, nil
}

func (s *Space[T]) List() *Dataframe[T] {
	df := newDataframe[T](s.name)
	stream, err := s.exec.Stream(s.name)
	df.SetStream(stream)
	df.SetError(err)
	return df
}

func (s *Space[T]) Set(key []byte, value T) error {
	if key == nil {
		return ErrKeyIsNil
	}
	return s.exec.Set(s.name, key, value)
}

func (s *Space[T]) Del(key []byte) error {
	if key == nil {
		return ErrKeyIsNil
	}
	return s.exec.Del(s.name, key)
}
