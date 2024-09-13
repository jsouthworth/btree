package treeset

import (
	"iter"

	"jsouthworth.net/go/btree"
)

type Set[T any] struct {
	impl *btree.BTree[T]
}

func Empty[T any](cmp func(a,b T) int) *Set[T] {
	return &Set[T]{
		impl: btree.Empty[T](
			cmp,
			func(a,b T) bool {
				return cmp(a,b) == 0
			},
		),
	}
}

func (s *Set[T]) Contains(elem T) bool {
	return s.impl.Contains(elem)
}

func (s *Set[T]) Add(elem T) *Set[T] {
	nimpl := s.impl.Add(elem)
	if nimpl == s.impl {
		return s
	}
	return &Set[T]{
		impl: nimpl,
	}
}

func (s *Set[T]) Remove(elem T) *Set[T] {
	nimpl := s.impl.Delete(elem)
	if nimpl == s.impl {
		return s
	}
	return &Set[T]{
		impl: nimpl,
	}
}

func (s *Set[T]) Len() int {
	return s.impl.Length()
}

func (s *Set[T]) All() iter.Seq[T] {
	i := s.Iterator()
	return i.Seq
}

func (s *Set[T]) From(elem T) iter.Seq[T] {
	i := s.IteratorFrom(elem)
	return i.Seq
}

func (s *Set[T]) Iterator() Iterator[T] {
	return Iterator[T]{
		impl: s.impl.Iterator(),
	}
}

func (s *Set[T]) IteratorFrom(elem T) Iterator[T] {
	return Iterator[T]{
		impl: s.impl.IteratorFrom(elem),
	}
}

func (s *Set[T]) AsTransient() *TSet[T] {
	return &TSet[T]{
		orig: s,
		impl: s.impl.AsTransient(),
	}
}

type TSet[T any] struct {
	orig *Set[T]
	impl *btree.TBTree[T]
}

func (s *TSet[T]) Contains(elem T) bool {
	return s.impl.Contains(elem)
}

func (s *TSet[T]) Add(elem T) *TSet[T] {
	s.impl.Add(elem)
	return s
}

func (s *TSet[T]) Remove(elem T) *TSet[T] {
	s.impl.Delete(elem)
	return s
}

func (s *TSet[T]) Len() int {
	return s.impl.Length()
}

func (s *TSet[T]) All() iter.Seq[T] {
	i := s.Iterator()
	return i.Seq
}

func (s *TSet[T]) From(elem T) iter.Seq[T] {
	i := s.IteratorFrom(elem)
	return i.Seq
}

func (s *TSet[T]) Iterator() Iterator[T] {
	return Iterator[T]{
		impl: s.impl.Iterator(),
	}
}

func (s *TSet[T]) IteratorFrom(elem T) Iterator[T] {
	return Iterator[T]{
		impl: s.impl.IteratorFrom(elem),
	}
}

func (s *TSet[T]) AsPersistent() *Set[T] {
	nimpl := s.impl.AsPersistent()
	if nimpl == s.orig.impl {
		return s.orig
	}
	return &Set[T]{
		impl: nimpl,
	}
}

type Iterator[T any] struct{
	impl btree.Iterator[T]
}

func (i *Iterator[T]) Seq(yield func(elem T) bool) {
	for i.HasNext() {
		if !yield(i.Next()) {
			break
		}
	}
}

func (i *Iterator[T]) Next() T {
	return i.impl.Next()
}

func (i *Iterator[T]) HasNext() bool {
	return i.impl.HasNext()
}
