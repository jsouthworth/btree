package btree

type stitcher[T any] struct {
	target []internalNodeEntry[T]
	offset int
}

func (s *stitcher[T]) copyAll(source []*nodeHeader[T], from, to int) {
	if to >= from {
		copy(s.target[s.offset:s.offset+(to-from)], source[from:to])
		s.offset += to - from
	}
}

func (s *stitcher[T]) copyOne(val *nodeHeader[T]) {
	s.target[s.offset] = internalNodeEntry[T]{val.maxKey(), val}
	s.offset++
}

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
	s.target[s.offset] = internalNodeEntry[T]{val.maxKey(), val}
	s.offset++
}
