package btree

type keyStitcher[T any] struct {
	target []T
	offset int
}

func (s *keyStitcher[T]) copyAll(source []T, from, to int) {
	if to >= from {
		copy(s.target[s.offset:s.offset+(to-from)], source[from:to])
		s.offset += to - from
	}
}

func (s *keyStitcher[T]) copyOne(val T) {
	s.target[s.offset] = val
	s.offset++
}

type nodeStitcher[T any] struct {
	target []node[T]
	offset int
}

func (s *nodeStitcher[T]) copyAll(source []node[T], from, to int) {
	if to >= from {
		copy(s.target[s.offset:s.offset+(to-from)], source[from:to])
		s.offset += to - from
	}
}

func (s *nodeStitcher[T]) copyOne(val node[T]) {
	s.target[s.offset] = val
	s.offset++
}
