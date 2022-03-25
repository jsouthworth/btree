package btree

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"unsafe"
)

type leafNode[T any] struct {
	nodeHeader[T]
	keys []T
}

func newLeaf[T any](len int, edit *atomic.Bool) *leafNode[T] {
	out := leafNode[T]{
		len:  len,
		edit: edit,
	}
	if edit.Load() {
		out.keys = make([]T, min(maxLen, len+expandLen))
	} else {
		out.keys = make([]T, len)
	}
	return &out
}

func (n *leafNode[T]) header() *nodeHeader[T] {
	return (*nodeHeader[T])(unsafe.Pointer(n))
}

func (n *leafNode[T]) isEditable() bool {
	return n.edit.Load()
}

func (n *leafNode[T]) leafPart() *leafNode[T] {
	return n
}

func (n *leafNode[T]) maxKey() T {
	return n.keys[n.len-1]
}

func (n *leafNode[T]) search(key T, cmp compareFunc[T]) int {
	i := sort.Search(n.len, func(i int) bool {
		return cmp(n.keys[i], key) >= 0
	})
	if i < n.len && cmp(key, n.keys[i]) == 0 {
		return i
	} else {
		return (-i) - 1
	}
}

func (n *leafNode[T]) searchFirst(key T, cmp compareFunc[T]) int {
	return sort.Search(n.len, func(i int) bool {
		return cmp(n.keys[i], key) >= 0
	})
}

func (n *leafNode[T]) searchEq(key T, cmp compareFunc[T], eq eqFunc[T]) (int, bool) {
	i := sort.Search(n.len, func(i int) bool {
		return cmp(n.keys[i], key) >= 0
	})
	if i < n.len && cmp(key, n.keys[i]) == 0 {
		valsEqual := eq(key, n.keys[i])
		if valsEqual {
			return i, false
		}
		return -i - 1, true
	} else {
		return (-i) - 1, false
	}
}

func (n *leafNode[T]) find(key T, cmp compareFunc[T]) (T, bool) {
	var out T
	v := n.search(key, cmp)
	if v >= 0 {
		out = n.keys[v]
	}
	return out, v >= 0
}

func (n *leafNode[T]) add(
	key T,
	cmp compareFunc[T],
	eq eqFunc[T],
	edit *atomic.Bool,
) (out nodeReturn[T]) {
	idx, replace := n.searchEq(key, cmp, eq)
	if idx >= 0 && !replace {
		return nodeReturn[T]{status: returnUnchanged}
	}
	ins := (-idx) - 1

	if n.isEditable() && (n.len < len(n.keys) || replace) {
		return n.modifyInPlace(ins, key, edit, replace)
	}

	if replace {
		return n.copyAndReplaceNode(ins, key, edit)
	}

	if n.len < maxLen {
		return n.copyAndInsertNode(ins, key, edit)
	}

	return n.split(ins, key, edit)
}

func (n *leafNode[T]) modifyInPlace(
	ins int, key T, edit *atomic.Bool, replace bool,
) nodeReturn[T] {
	if replace {
		n.keys[ins] = key
		return nodeReturn[T]{status: returnReplaced, nodes: [3]*nodeHeader[T]{n}}
	} else if ins == n.len {
		n.keys[n.len] = key
		n.len++
		return nodeReturn[T]{status: returnOne, nodes: [3]*nodeHeader[T]{n}}
	} else {
		copy(n.keys[ins+1:], n.keys[ins:n.len])
		n.keys[ins] = key
		n.len++
		return nodeReturn[T]{status: returnEarly}
	}
}

func (n *leafNode[T]) copyAndInsertNode(
	ins int, key T, edit *atomic.Bool,
) nodeReturn[T] {
	nl := newLeaf[T](n.len+1, edit)
	ks := keyStitcher[T]{nl.keys, 0}
	ks.copyAll(n.keys, 0, ins)
	ks.copyOne(key)
	ks.copyAll(n.keys, ins, n.len)
	return nodeReturn[T]{status: returnOne, nodes: [3]*nodeHeader[T]{nl}}
}

func (n *leafNode[T]) copyAndReplaceNode(
	ins int, key T, edit *atomic.Bool,
) nodeReturn[T] {
	nl := newLeaf[T](n.len, edit)
	copy(nl.keys, n.keys)
	nl.keys[ins] = key
	return nodeReturn[T]{status: returnReplaced, nodes: [3]*nodeHeader[T]{nl}}
}

func (n *leafNode[T]) split(
	ins int, key T, edit *atomic.Bool,
) nodeReturn[T] {
	firstHalf := (n.len + 1) >> 1
	secondHalf := n.len + 1 - firstHalf
	n1 := newLeaf[T](firstHalf, edit)
	n2 := newLeaf[T](secondHalf, edit)

	if ins < firstHalf {
		ks := keyStitcher[T]{n1.keys, 0}
		ks.copyAll(n.keys, 0, ins)
		ks.copyOne(key)
		ks.copyAll(n.keys, ins, firstHalf-1)
		copy(n2.keys, n.keys[firstHalf-1:n.len])
		return nodeReturn[T]{status: returnTwo, nodes: [3]*nodeHeader[T]{n1, n2}}
	}

	copy(n1.keys, n.keys[0:firstHalf])
	ks := keyStitcher[T]{n2.keys, 0}
	ks.copyAll(n.keys, firstHalf, ins)
	ks.copyOne(key)
	ks.copyAll(n.keys, ins, n.len)
	return nodeReturn[T]{status: returnTwo, nodes: [3]*nodeHeader[T]{n1, n2}}
}

func (n *leafNode[T]) remove(
	key T,
	leftNode, rightNode *nodeHeader[T],
	cmp compareFunc[T],
	edit *atomic.Bool,
) (out nodeReturn[T]) {
	idx := n.search(key, cmp)
	if idx < 0 {
		return nodeReturn[T]{status: returnUnchanged}
	}

	newLen := n.len - 1

	var left, right *leafNode[T]
	if leftNode != nil {
		left = leftNode.leafPart()
	}
	if rightNode != nil {
		right = rightNode.leafPart()
	}

	switch {
	case !n.needsMerge(newLen, left, right):
		if n.isEditable() {
			return n.removeInPlace(idx, newLen, left, right, edit)
		}
		return n.copyAndRemoveIdx(idx, newLen, left, right, edit)
	case left.canJoin(newLen):
		return n.joinLeft(idx, newLen, left, right, edit)
	case right.canJoin(newLen):
		return n.joinRight(idx, newLen, left, right, edit)
	case left != nil &&
		(left.isEditable() || right == nil || left.len >= right.len):
		return n.borrowLeft(idx, newLen, left, right, edit)
	case right != nil:
		return n.borrowRight(idx, newLen, left, right, edit)
	default:
		panic("unreachable")
	}
}

func (n *leafNode[T]) needsMerge(
	newLen int,
	left, right *leafNode[T],
) bool {
	return newLen < minLen && (left != nil || right != nil)
}

func (n *leafNode[T]) removeInPlace(
	idx, newLen int,
	left, right *leafNode[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	copy(n.keys[idx:], n.keys[idx+1:n.len])
	n.len = newLen
	if idx == newLen {
		return nodeReturn[T]{
			status: returnThree,
			nodes: [...]*nodeHeader[T]{
				leafNodeToNode(left),
				n,
				leafNodeToNode(right),
			},
		}
	}
	return nodeReturn[T]{status: returnEarly}
}

func (n *leafNode[T]) copyAndRemoveIdx(
	idx, newLen int,
	left, right *leafNode[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	center := newLeaf[T](newLen, edit)
	copy(center.keys, n.keys[0:idx])
	copy(center.keys[idx:], n.keys[idx+1:])
	return nodeReturn[T]{
		status: returnThree,
		nodes: [...]*nodeHeader[T]{
			leafNodeToNode(left),
			center,
			leafNodeToNode(right),
		},
	}
}

func (n *leafNode[T]) joinLeft(
	idx, newLen int,
	left, right *leafNode[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	join := newLeaf[T](left.len+newLen, edit)
	ks := keyStitcher[T]{join.keys, 0}
	ks.copyAll(left.keys, 0, left.len)
	ks.copyAll(n.keys, 0, idx)
	ks.copyAll(n.keys, idx+1, n.len)
	return nodeReturn[T]{
		status: returnThree,
		nodes:  [...]*nodeHeader[T]{nil, join, leafNodeToNode(right)},
	}
}

func (n *leafNode[T]) joinRight(
	idx, newLen int,
	left, right *leafNode[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	join := newLeaf[T](right.len+newLen, edit)
	ks := keyStitcher[T]{join.keys, 0}
	ks.copyAll(n.keys, 0, idx)
	ks.copyAll(n.keys, idx+1, n.len)
	ks.copyAll(right.keys, 0, right.len)
	return nodeReturn[T]{
		status: returnThree,
		nodes:  [...]*nodeHeader[T]{leafNodeToNode(left), join, nil},
	}
}

func (n *leafNode[T]) canJoin(newLen int) bool {
	return n != nil && (n.len+newLen) < maxLen
}

func (n *leafNode[T]) borrowLeft(
	idx, newLen int,
	left, right *leafNode[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	var (
		totalLen     = left.len + newLen
		newLeftLen   = totalLen >> 1
		newCenterLen = totalLen - newLeftLen
		leftTail     = left.len - newLeftLen
	)

	var newLeft, newCenter *leafNode[T]

	// prepend to center
	if n.isEditable() && newCenterLen <= len(n.keys) {
		newCenter = n
		copy(n.keys[leftTail+idx:], n.keys[idx+1:n.len])
		copy(n.keys[leftTail:], n.keys[0:idx])
		copy(n.keys[0:], left.keys[newLeftLen:left.len])
		n.len = newCenterLen
	} else {
		newCenter = newLeaf[T](newCenterLen, edit)
		ks := keyStitcher[T]{newCenter.keys, 0}
		ks.copyAll(left.keys, newLeftLen, left.len)
		ks.copyAll(n.keys, 0, idx)
		ks.copyAll(n.keys, idx+1, n.len)
	}

	// shrink left
	if left.isEditable() {
		newLeft = left
		left.len = newLeftLen
	} else {
		newLeft = newLeaf[T](newLeftLen, edit)
		copy(newLeft.keys, left.keys[0:newLeftLen])
	}

	return nodeReturn[T]{
		status: returnThree,
		nodes:  [...]*nodeHeader[T]{newLeft, newCenter, leafNodeToNode(right)},
	}
}

func (n *leafNode[T]) borrowRight(
	idx, newLen int,
	left, right *leafNode[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	var (
		totalLen     = newLen + right.len
		newCenterLen = totalLen >> 1
		newRightLen  = totalLen - newCenterLen
		rightHead    = right.len - newRightLen
	)

	var newCenter, newRight *leafNode[T]

	// append to center
	if n.isEditable() && newCenterLen <= len(n.keys) {
		newCenter = n
		ks := keyStitcher[T]{n.keys, idx}
		ks.copyAll(n.keys, idx+1, n.len)
		ks.copyAll(right.keys, 0, rightHead)
		n.len = newCenterLen
	} else {
		newCenter = newLeaf[T](newCenterLen, edit)
		ks := keyStitcher[T]{newCenter.keys, 0}
		ks.copyAll(n.keys, 0, idx)
		ks.copyAll(n.keys, idx+1, n.len)
		ks.copyAll(right.keys, 0, rightHead)
	}

	//cut head from right
	if right.isEditable() {
		newRight = right
		copy(right.keys, right.keys[rightHead:right.len])
		right.len = newRightLen
	} else {
		newRight = newLeaf[T](newRightLen, edit)
		copy(newRight.keys, right.keys[rightHead:right.len])
	}
	return nodeReturn[T]{
		status: returnThree,
		nodes:  [...]*nodeHeader[T]{leafNodeToNode(left), newCenter, newRight},
	}
}

func (n *leafNode[T]) String() string {
	var b strings.Builder
	n.string(&b, 0)
	return b.String()
}

func (n *leafNode[T]) string(b *strings.Builder, lvl int) {
	b.WriteRune('{')
	for i := 0; i < n.len; i++ {
		if i > 0 {
			b.WriteRune(' ')
		}
		fmt.Fprintf(b, "%v", n.keys[i])
	}
	b.WriteRune('}')
}

func leafNodeToNode[T any](n *leafNode[T]) *nodeHeader[T] {
	if n != nil {
		return n.header()
	}
	return nil
}
