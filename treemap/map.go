package treemap

import (
	"iter"
	
	"jsouthworth.net/go/btree"
)

type Map[K, V any] struct {
	impl *btree.BTree[entry[K,V]]
}

func Empty[K,V any](cmp func(a,b K) int, eq func(a,b V) bool) *Map[K,V] {
	return &Map[K,V]{
		impl: btree.Empty[entry[K,V]](
			func(a,b entry[K,V]) int {
				return cmp(a.key, b.key)
			},
			func(a,b entry[K,V]) bool {
				return cmp(a.key, b.key) == 0 &&
					eq(a.value, b.value)
			},
		),
	}
}

func (m *Map[K,V]) Contains(key K) bool {
	return m.impl.Contains(entry[K,V]{key:key})
}

func (m *Map[K,V]) At(key K) V {
	e := m.impl.At(entry[K,V]{key: key})
	return e.value
}

func (m *Map[K,V]) Find(key K) (V, bool) {
	e, ok := m.impl.Find(entry[K,V]{key:key})
	return e.value, ok
}

func (m *Map[K,V]) Assoc(key K, value V) *Map[K,V] {
	nimpl := m.impl.Add(entry[K,V]{key:key, value:value})
	if nimpl == m.impl {
		return m
	}
	return &Map[K,V]{
		impl: nimpl,
	}
}

func (m *Map[K,V]) Delete(key K) *Map[K,V] {
	nimpl := m.impl.Delete(entry[K,V]{key:key})
	if nimpl == m.impl {
		return m
	}
	return &Map[K,V]{
		impl: nimpl,
	}
}

func (m *Map[K,V]) Len(key K) int {
	return m.impl.Length()
}

func (m *Map[K,V]) All() iter.Seq2[K,V] {
	i := m.Iterator()
	return i.Seq2
}

func (m *Map[K,V]) From(key K) iter.Seq2[K,V] {
	i := m.IteratorFrom(key)
	return i.Seq2
}

func (m *Map[K,V]) Iterator() Iterator[K,V] {
	return Iterator[K,V]{
		impl: m.impl.Iterator(),
	}
}

func (m *Map[K,V]) IteratorFrom(key K) Iterator[K,V] {
	return Iterator[K,V]{
		impl: m.impl.IteratorFrom(entry[K,V]{key:key}),
	}
}

func (m *Map[K,V]) AsTransient() *TMap[K,V] {
	return &TMap[K,V]{
		orig: m,
		impl: m.impl.AsTransient(),
	}
}

type TMap[K, V any] struct {
	orig *Map[K,V]
	impl *btree.TBTree[entry[K,V]]
}

func (m *TMap[K,V]) Contains(key K) bool {
	return m.impl.Contains(entry[K,V]{key:key})
}

func (m *TMap[K,V]) At(key K) V {
	e := m.impl.At(entry[K,V]{key: key})
	return e.value
}

func (m *TMap[K,V]) Find(key K) (V, bool) {
	e, ok := m.impl.Find(entry[K,V]{key:key})
	return e.value, ok
}

func (m *TMap[K,V]) Assoc(key K, value V) *TMap[K,V] {
	m.impl.Add(entry[K,V]{key:key, value:value})
	return m
}

func (m *TMap[K,V]) Delete(key K) *TMap[K,V] {
	m.impl.Delete(entry[K,V]{key: key})
	return m
}

func (m *TMap[K,V]) Len(key K) int {
	return m.impl.Length()
}

func (m *TMap[K,V]) All() iter.Seq2[K,V] {
	i := m.Iterator()
	return i.Seq2
}

func (m *TMap[K,V]) From(key K) iter.Seq2[K,V] {
	i := m.IteratorFrom(key)
	return i.Seq2
}

func (m *TMap[K,V]) Iterator() Iterator[K,V] {
	return Iterator[K,V]{
		impl: m.impl.Iterator(),
	}
}

func (m *TMap[K,V]) IteratorFrom(key K) Iterator[K,V] {
	return Iterator[K,V]{
		impl: m.impl.IteratorFrom(entry[K,V]{key:key}),
	}
}

func (m *TMap[K,V]) AsPersistent() *Map[K,V] {
	nimpl := m.impl.AsPersistent()
	if nimpl == m.orig.impl {
		return m.orig
	}
	return &Map[K,V]{
		impl: nimpl,
	}
}

type Iterator[K,V any] struct {
	impl btree.Iterator[entry[K,V]]
}

func (i *Iterator[K,V]) Seq2(yield func(key K, value V) bool) {
	for i.HasNext() {
		k, v := i.Next()
		if !yield(k,v) {
			break;
		}
	}
}

func (i *Iterator[K,V]) Next() (K, V) {
	e := i.impl.Next()
	return e.key, e.value
}

func (i *Iterator[K,V]) HasNext() bool {
	return i.impl.HasNext()
}

type entry[K, V any] struct {
	key K
	value V
}

