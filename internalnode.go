package btree

import (
	"fmt"
	"strings"
	"sync/atomic"
	"unsafe"
)

type nodeKind uint8

const (
	nodeKindInternal nodeKind = iota
	nodeKindLeaf
)

type nodeHeader[T any] struct {
	kind nodeKind
	size int8
	edit *atomic.Bool
}

func (h *nodeHeader[T]) asInternalNode() *internalNode[T] {
	return (*internalNode[T])(unsafe.Pointer(h))
}

func (h *nodeHeader[T]) asLeafNode() *leafNode[T] {
	return (*leafNode[T])(unsafe.Pointer(h))
}

func (h *nodeHeader[T]) search(key T, cmp compareFunc[T]) int {
	if h.kind == nodeKindLeaf {
		return h.asLeafNode().search(key, cmp)
	}
	return h.asInternalNode().search(key, cmp)
}

func (h *nodeHeader[T]) searchFirst(key T, cmp compareFunc[T]) int {
	if h.kind == nodeKindLeaf {
		return h.asLeafNode().searchFirst(key, cmp)
	}
	return h.asInternalNode().searchFirst(key, cmp)
}

func (h *nodeHeader[T]) find(key T, cmp compareFunc[T]) (T, bool) {
	if h.kind == nodeKindLeaf {
		return h.asLeafNode().find(key, cmp)
	}
	return h.asInternalNode().find(key, cmp)
}

func (h *nodeHeader[T]) add(key T, cmp compareFunc[T], eq eqFunc[T], edit *atomic.Bool) nodeReturn[T] {
	if h.kind == nodeKindLeaf {
		return h.asLeafNode().add(key, cmp, eq, edit)
	}
	return h.asInternalNode().add(key, cmp, eq, edit)
}

func (h *nodeHeader[T]) remove(key T, left, right *nodeHeader[T], cmp compareFunc[T], edit *atomic.Bool) nodeReturn[T] {
	if h.kind == nodeKindLeaf {
		return h.asLeafNode().remove(key, left, right, cmp, edit)
	}
	return h.asInternalNode().remove(key, left, right, cmp, edit)
}

func (h *nodeHeader[T]) maxKey() T {
	if h.kind == nodeKindLeaf {
		return h.asLeafNode().maxKey()
	}
	return h.asInternalNode().maxKey()
}

func (h *nodeHeader[T]) string(b *strings.Builder, lvl int) {
	if h.kind == nodeKindLeaf {
		return h.asLeafNode().string(b, lvl)
	}
	return h.asInternalNode().string(b, lvl)
}

type internalNodeEntry[T any] struct {
	key   T
	entry *nodeHeader[T]
}

type internalNode[T any] struct {
	nodeHeader[T]
	children []internalNodeEntry[T]
}

func newNode[T any](len int, edit *atomic.Bool) *internalNode[T] {
	return &internalNode[T]{
		kind:     nodeKindInternal,
		size:     len,
		edit:     edit,
		children: make([]internalNodeEntry[T], len),
	}
}

func (n *internalNode[T]) header() *nodeHeader[T] {
	return (*nodeHeader[T])(unsafe.Pointer(n))
}

func (n *internalNode[T]) find(key T, cmp compareFunc[T]) (T, bool) {
	var zeroVal T
	idx := n.search(key, cmp)
	if idx >= 0 {
		return n.keys[idx], true
	}
	idx = -idx - 1
	if idx == n.len {
		return zeroVal, false
	}
	return n.children[idx].find(key, cmp)
}

func (n *internalNode[T]) add(
	key T,
	cmp compareFunc[T],
	eq eqFunc[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	idx, _ := n.searchEq(key, cmp, eq)
	if idx >= 0 {
		return nodeReturn[T]{status: returnUnchanged}
	}
	ins := -idx - 1
	if ins == n.len {
		ins = n.len - 1
	}
	ret := n.children[ins].add(key, cmp, eq, edit)
	switch ret.status {
	case returnUnchanged:
		return ret
	case returnEarly:
		return ret
	case returnOne, returnReplaced:
		if n.isEditable() {
			return n.modifyInPlace(ins, eq, ret.nodes[0], ret.status)
		}
		return n.copyAndModify(ins, eq, edit, ret.nodes[0], ret.status)
	default:
		if n.len < maxLen {
			return n.copyAndAppend(
				ins, ret.nodes[0], ret.nodes[1], edit)
		}
		return n.split(ins, ret.nodes[0], ret.nodes[1], edit)
	}
}

func (n *internalNode[T]) maxKey() T {
	return n.children[n.len-1].key
}

func (n *internalNode[T]) modifyInPlace(
	ins int, eq eqFunc[T], new *nodeHeader[T], status returnStatus,
) nodeReturn[T] {
	n.keys[ins] = new.maxKey()
	n.children[ins] = new
	if ins == n.len-1 && eq(new.maxKey(), n.maxKey()) {
		return nodeReturn[T]{
			status: status,
			nodes:  [3]*nodeHeader[T]{n},
		}
	}
	if status == returnReplaced {
		return nodeReturn[T]{
			status: status,
			nodes:  [3]*nodeHeader[T]{n},
		}
	}
	return nodeReturn[T]{status: returnEarly}
}

func (n *internalNode[T]) copyAndModify(
	ins int,
	eq eqFunc[T],
	edit *atomic.Bool,
	newNode *nodeHeader[T],
	status returnStatus,
) nodeReturn[T] {
	var newKeys []T
	if eq(newNode.maxKey(), n.keys[ins]) {
		newKeys = n.keys
	} else {
		newKeys = make([]T, n.len)
		copy(newKeys, n.keys)
		newKeys[ins] = newNode.maxKey()
	}

	var newChildren []*nodeHeader[T]
	if newNode == n.children[ins] {
		newChildren = n.children
	} else {
		newChildren = make([]*nodeHeader[T], n.len)
		copy(newChildren, n.children)
		newChildren[ins] = newNode
	}
	return nodeReturn[T]{
		status: status,
		nodes: [3]*nodeHeader[T]{
			&internalNode[T]{
				leafNode: &leafNode[T]{
					keys: newKeys,
					len:  n.len,
					edit: edit,
				},
				children: newChildren,
			},
		},
	}
}

func (n *internalNode[T]) copyAndAppend(
	ins int,
	n1, n2 *nodeHeader[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	newNode := newNode[T](n.len+1, edit)
	nstitch := stitcher[T]{newNode.children, 0}
	nstitch.copyAll(n.children, 0, ins)
	nstitch.copyOne(n1)
	nstitch.copyOne(n2)
	nstitch.copyAll(n.children, ins+1, n.len)

	return nodeReturn[T]{
		status: returnOne,
		nodes:  [3]*nodeHeader[T]{newNode},
	}
}

func (n *internalNode[T]) split(
	ins int,
	n1, n2 *nodeHeader[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	half1 := (n.len + 1) >> 1
	if ins+1 == half1 {
		half1++
	}
	half2 := n.len + 1 - half1

	node1 := newNode[T](half1, edit)
	node2 := newNode[T](half2, edit)

	// add to first half
	if ins < half1 {
		ns := stitcher[T]{node1.children, 0}
		ns.copyAll(n.children, 0, ins)
		ns.copyOne(n1)
		ns.copyOne(n2)
		ns.copyAll(n.children, ins+1, half1-1)
		copy(node2.children, n.children[half1-1:n.len])

		return nodeReturn[T]{
			status: returnTwo,
			nodes: [3]*nodeHeader[T]{
				node1,
				node2,
			},
		}
	}

	// add to second half
	copy(node1.children, n.children[0:half1])
	ns := stitcher[T]{node2.children, 0}
	ns.copyAll(n.children, half1, ins)
	ns.copyOne(n1)
	ns.copyOne(n2)
	ns.copyAll(n.children, ins+1, n.len)

	return nodeReturn[T]{
		status: returnTwo,
		nodes: [3]*nodeHeader[T]{
			node1,
			node2,
		},
	}
}

func (n *internalNode[T]) remove(
	key T,
	leftNode, rightNode *nodeHeader[T],
	cmp compareFunc[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	var left, right *internalNode[T]
	if leftNode != nil {
		left = leftNode.(*internalNode[T])
	}
	if rightNode != nil {
		right = rightNode.(*internalNode[T])
	}
	return n.removeInternal(
		key, left, right, cmp, edit)
}

func (n *internalNode[T]) removeInternal(
	key T,
	left, right *internalNode[T],
	cmp compareFunc[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	idx := n.search(key, cmp)
	if idx < 0 {
		idx = -idx - 1
	}
	if idx == n.len {
		return nodeReturn[T]{status: returnUnchanged}
	}

	var leftChild *nodeHeader[T]
	if idx > 0 {
		leftChild = n.children[idx-1]
	}
	var rightChild *nodeHeader[T]
	if idx < n.len-1 {
		rightChild = n.children[idx+1]
	}

	ret := n.children[idx].remove(key, leftChild, rightChild, cmp, edit)
	switch ret.status {
	case returnUnchanged:
		return ret
	case returnEarly:
		return ret
	}

	newLen := n.len - 1
	if leftChild != nil {
		newLen -= 1
	}
	if rightChild != nil {
		newLen -= 1
	}
	if ret.nodes[0] != nil {
		newLen += 1
	}
	if ret.nodes[1] != nil {
		newLen += 1
	}
	if ret.nodes[2] != nil {
		newLen += 1
	}

	switch {
	case !n.needsRebalance(newLen, left, right):
		if n.isEditable() && idx < n.len-2 {
			return n.removeInPlace(
				idx, newLen, left, right, edit, ret.nodes)
		}
		return n.copyAndRemoveIdx(
			idx, newLen, left, right, edit, ret.nodes)
	case left != nil && left.canJoin(newLen):
		return n.joinLeft(idx, newLen, left, right, edit, ret.nodes)
	case right != nil && right.canJoin(newLen):
		return n.joinRight(idx, newLen, left, right, edit, ret.nodes)
	case left != nil && (right == nil || left.len >= right.len):
		return n.borrowLeft(idx, newLen, left, right, edit, ret.nodes)
	case right != nil:
		return n.borrowRight(idx, newLen, left, right, edit, ret.nodes)
	default:
		panic("unreachable")
	}
}

func (n *internalNode[T]) needsRebalance(
	newLen int,
	left, right *internalNode[T],
) bool {
	return newLen < minLen && (left != nil || right != nil)
}

func (n *internalNode[T]) removeInPlace(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]*nodeHeader[T],
) nodeReturn[T] {
	cs := stitcher[T]{n.children, max(idx-1, 0)}
	if nodes[0] != nil {
		cs.copyOne(nodes[0])
	}
	cs.copyOne(nodes[1])
	if nodes[2] != nil {
		cs.copyOne(nodes[2])
	}
	if newLen != n.len {
		cs.copyAll(n.children, idx+2, n.len)
	}

	n.len = newLen
	return nodeReturn[T]{status: returnEarly}
}

func (n *internalNode[T]) copyAndRemoveIdx(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]*nodeHeader[T],
) nodeReturn[T] {
	newCenter := newNode[T](newLen, edit)
	cs := stitcher[T]{newCenter.children, 0}
	cs.copyAll(n.children, 0, idx-1)
	if nodes[0] != nil {
		cs.copyOne(nodes[0])
	}
	cs.copyOne(nodes[1])
	if nodes[2] != nil {
		cs.copyOne(nodes[2])
	}
	cs.copyAll(n.children, idx+2, n.len)

	return nodeReturn[T]{
		status: returnThree,
		nodes: [3]*nodeHeader[T]{
			internalNodeToNode(left),
			newCenter,
			internalNodeToNode(right),
		},
	}
}

func (n *internalNode[T]) joinLeft(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]*nodeHeader[T],
) nodeReturn[T] {
	join := newNode[T](left.len+newLen, edit)

	cs := stitcher[T]{join.children, 0}
	cs.copyAll(left.children, 0, left.len)
	cs.copyAll(n.children, 0, idx-1)
	if nodes[0] != nil {
		cs.copyOne(nodes[0])
	}
	cs.copyOne(nodes[1])
	if nodes[2] != nil {
		cs.copyOne(nodes[2])
	}
	cs.copyAll(n.children, idx+2, n.len)

	return nodeReturn[T]{
		status: returnThree,
		nodes:  [3]*nodeHeader[T]{nil, join, internalNodeToNode(right)},
	}
}

func (n *internalNode[T]) joinRight(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]*nodeHeader[T],
) nodeReturn[T] {
	join := newNode[T](newLen+right.len, edit)

	cs := stitcher[T]{join.children, 0}
	cs.copyAll(n.children, 0, idx-1)
	if nodes[0] != nil {
		cs.copyOne(nodes[0])
	}
	cs.copyOne(nodes[1])
	if nodes[2] != nil {
		cs.copyOne(nodes[2])
	}
	cs.copyAll(n.children, idx+2, n.len)
	cs.copyAll(right.children, 0, right.len)

	return nodeReturn[T]{
		status: returnThree,
		nodes:  [3]*nodeHeader[T]{internalNodeToNode(left), join, nil},
	}
}

func (n *internalNode[T]) borrowLeft(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]*nodeHeader[T],
) nodeReturn[T] {
	var (
		totalLen     = left.len + newLen
		newLeftLen   = totalLen >> 1
		newCenterLen = totalLen - newLeftLen
	)

	newLeft := newNode[T](newLeftLen, edit)
	newCenter := newNode[T](newCenterLen, edit)

	copy(newLeft.children, left.children[0:newLeftLen])

	cs := stitcher[T]{newCenter.children, 0}
	cs.copyAll(left.children, newLeftLen, left.len)
	cs.copyAll(n.children, 0, idx-1)
	if nodes[0] != nil {
		cs.copyOne(nodes[0])
	}
	cs.copyOne(nodes[1])
	if nodes[2] != nil {
		cs.copyOne(nodes[2])
	}
	cs.copyAll(n.children, idx+2, n.len)

	return nodeReturn[T]{
		status: returnThree,
		nodes:  [3]*nodeHeader[T]{newLeft, newCenter, internalNodeToNode(right)},
	}
}

func (n *internalNode[T]) borrowRight(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]*nodeHeader[T],
) nodeReturn[T] {
	var (
		totalLen     = newLen + right.len
		newCenterLen = totalLen >> 1
		newRightLen  = totalLen - newCenterLen
		rightHead    = right.len - newRightLen
	)

	newCenter := newNode[T](newCenterLen, edit)
	newRight := newNode[T](newRightLen, edit)

	cs := stitcher[T]{newCenter.children, 0}
	cs.copyAll(n.children, 0, idx-1)
	if nodes[0] != nil {
		cs.copyOne(nodes[0])
	}
	cs.copyOne(nodes[1])
	if nodes[2] != nil {
		cs.copyOne(nodes[2])
	}
	cs.copyAll(n.children, idx+2, n.len)
	cs.copyAll(right.children, 0, rightHead)

	copy(newRight.children, right.children[rightHead:right.len])

	return nodeReturn[T]{
		status: returnThree,
		nodes:  [3]*nodeHeader[T]{internalNodeToNode(left), newCenter, newRight},
	}
}

func (n *internalNode[T]) String() string {
	var b strings.Builder
	n.string(&b, 0)
	return b.String()
}

func (n *internalNode[T]) string(b *strings.Builder, lvl int) {
	for i := 0; i < n.len; i++ {
		b.WriteString("\n")
		for j := 0; j < lvl; j++ {
			b.WriteString("| ")
		}
		fmt.Fprintf(b, "%v: ", n.keys[i])
		n.children[i].string(b, lvl+1)
	}
}

func internalNodeToNode[T any](n *internalNode[T]) *nodeHeader[T] {
	if n != nil {
		return n.header()
	}
	return nil
}
