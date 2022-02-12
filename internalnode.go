package btree

import (
	"fmt"
	"strings"

	"jsouthworth.net/go/btree/internal/atomic"
)

type internalNode[T any] struct {
	*leafNode[T]

	children []node[T]
}

func newNode[T any](len int, edit *atomic.Bool) *internalNode[T] {
	return &internalNode[T]{
		leafNode: &leafNode[T]{
			keys: make([]T, len),
			len:  len,
			edit: edit,
		},
		children: make([]node[T], len),
	}
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

func (n *internalNode[T]) modifyInPlace(
	ins int, eq eqFunc[T], new node[T], status returnStatus,
) nodeReturn[T] {
	n.keys[ins] = new.maxKey()
	n.children[ins] = new
	if ins == n.len-1 && eq(new.maxKey(), n.maxKey()) {
		return nodeReturn[T]{
			status: status,
			nodes:  [3]node[T]{n},
		}
	}
	if status == returnReplaced {
		return nodeReturn[T]{
			status: status,
			nodes:  [3]node[T]{n},
		}
	}
	return nodeReturn[T]{status: returnEarly}
}

func (n *internalNode[T]) copyAndModify(
	ins int,
	eq eqFunc[T],
	edit *atomic.Bool,
	newNode node[T],
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

	var newChildren []node[T]
	if newNode == n.children[ins] {
		newChildren = n.children
	} else {
		newChildren = make([]node[T], n.len)
		copy(newChildren, n.children)
		newChildren[ins] = newNode
	}
	return nodeReturn[T]{
		status: status,
		nodes: [3]node[T]{
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
	n1, n2 node[T],
	edit *atomic.Bool,
) nodeReturn[T] {
	newNode := newNode[T](n.len+1, edit)
	kstitch := keyStitcher[T]{newNode.keys, 0}
	kstitch.copyAll(n.keys, 0, ins)
	kstitch.copyOne(n1.maxKey())
	kstitch.copyOne(n2.maxKey())
	kstitch.copyAll(n.keys, ins+1, n.len)

	nstitch := nodeStitcher[T]{newNode.children, 0}
	nstitch.copyAll(n.children, 0, ins)
	nstitch.copyOne(n1)
	nstitch.copyOne(n2)
	nstitch.copyAll(n.children, ins+1, n.len)

	return nodeReturn[T]{
		status: returnOne,
		nodes:  [3]node[T]{newNode},
	}
}

func (n *internalNode[T]) split(
	ins int,
	n1, n2 node[T],
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
		ks := keyStitcher[T]{node1.keys, 0}
		ks.copyAll(n.keys, 0, ins)
		ks.copyOne(n1.maxKey())
		ks.copyOne(n2.maxKey())
		ks.copyAll(n.keys, ins+1, half1-1)
		copy(node2.keys, n.keys[half1-1:n.len])

		ns := nodeStitcher[T]{node1.children, 0}
		ns.copyAll(n.children, 0, ins)
		ns.copyOne(n1)
		ns.copyOne(n2)
		ns.copyAll(n.children, ins+1, half1-1)
		copy(node2.children, n.children[half1-1:n.len])

		return nodeReturn[T]{
			status: returnTwo,
			nodes: [3]node[T]{
				node1,
				node2,
			},
		}
	}

	// add to second half
	copy(node1.keys, n.keys[0:half1])
	ks := keyStitcher[T]{node2.keys, 0}
	ks.copyAll(n.keys, half1, ins)
	ks.copyOne(n1.maxKey())
	ks.copyOne(n2.maxKey())
	ks.copyAll(n.keys, ins+1, n.len)

	copy(node1.children, n.children[0:half1])
	ns := nodeStitcher[T]{node2.children, 0}
	ns.copyAll(n.children, half1, ins)
	ns.copyOne(n1)
	ns.copyOne(n2)
	ns.copyAll(n.children, ins+1, n.len)

	return nodeReturn[T]{
		status: returnTwo,
		nodes: [3]node[T]{
			node1,
			node2,
		},
	}
}

func (n *internalNode[T]) remove(
	key T,
	leftNode, rightNode node[T],
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

	var leftChild node[T]
	if idx > 0 {
		leftChild = n.children[idx-1]
	}
	var rightChild node[T]
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
	nodes [3]node[T],
) nodeReturn[T] {
	ks := keyStitcher[T]{n.keys, max(idx-1, 0)}
	if nodes[0] != nil {
		ks.copyOne(nodes[0].maxKey())
	}
	ks.copyOne(nodes[1].maxKey())
	if nodes[2] != nil {
		ks.copyOne(nodes[2].maxKey())
	}
	if newLen != n.len {
		ks.copyAll(n.keys, idx+2, n.len)
	}

	cs := nodeStitcher[T]{n.children, max(idx-1, 0)}
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
	nodes [3]node[T],
) nodeReturn[T] {
	newCenter := newNode[T](newLen, edit)

	ks := keyStitcher[T]{newCenter.keys, 0}
	ks.copyAll(n.keys, 0, idx-1)
	if nodes[0] != nil {
		ks.copyOne(nodes[0].maxKey())
	}
	ks.copyOne(nodes[1].maxKey())
	if nodes[2] != nil {
		ks.copyOne(nodes[2].maxKey())
	}
	ks.copyAll(n.keys, idx+2, n.len)

	cs := nodeStitcher[T]{newCenter.children, 0}
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
		nodes: [3]node[T]{
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
	nodes [3]node[T],
) nodeReturn[T] {
	join := newNode[T](left.len+newLen, edit)

	ks := keyStitcher[T]{join.keys, 0}
	ks.copyAll(left.keys, 0, left.len)
	ks.copyAll(n.keys, 0, idx-1)
	if nodes[0] != nil {
		ks.copyOne(nodes[0].maxKey())
	}
	ks.copyOne(nodes[1].maxKey())
	if nodes[2] != nil {
		ks.copyOne(nodes[2].maxKey())
	}
	ks.copyAll(n.keys, idx+2, n.len)

	cs := nodeStitcher[T]{join.children, 0}
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
		nodes:  [3]node[T]{nil, join, internalNodeToNode(right)},
	}
}

func (n *internalNode[T]) joinRight(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]node[T],
) nodeReturn[T] {
	join := newNode[T](newLen+right.len, edit)

	ks := keyStitcher[T]{join.keys, 0}
	ks.copyAll(n.keys, 0, idx-1)
	if nodes[0] != nil {
		ks.copyOne(nodes[0].maxKey())
	}
	ks.copyOne(nodes[1].maxKey())
	if nodes[2] != nil {
		ks.copyOne(nodes[2].maxKey())
	}
	ks.copyAll(n.keys, idx+2, n.len)
	ks.copyAll(right.keys, 0, right.len)

	cs := nodeStitcher[T]{join.children, 0}
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
		nodes:  [3]node[T]{internalNodeToNode(left), join, nil},
	}
}

func (n *internalNode[T]) borrowLeft(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]node[T],
) nodeReturn[T] {
	var (
		totalLen     = left.len + newLen
		newLeftLen   = totalLen >> 1
		newCenterLen = totalLen - newLeftLen
	)

	newLeft := newNode[T](newLeftLen, edit)
	newCenter := newNode[T](newCenterLen, edit)

	copy(newLeft.keys, left.keys[0:newLeftLen])

	ks := keyStitcher[T]{newCenter.keys, 0}
	ks.copyAll(left.keys, newLeftLen, left.len)
	ks.copyAll(n.keys, 0, idx-1)
	if nodes[0] != nil {
		ks.copyOne(nodes[0].maxKey())
	}
	ks.copyOne(nodes[1].maxKey())
	if nodes[2] != nil {
		ks.copyOne(nodes[2].maxKey())
	}
	ks.copyAll(n.keys, idx+2, n.len)

	copy(newLeft.children, left.children[0:newLeftLen])

	cs := nodeStitcher[T]{newCenter.children, 0}
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
		nodes:  [3]node[T]{newLeft, newCenter, internalNodeToNode(right)},
	}
}

func (n *internalNode[T]) borrowRight(
	idx int,
	newLen int,
	left, right *internalNode[T],
	edit *atomic.Bool,
	nodes [3]node[T],
) nodeReturn[T] {
	var (
		totalLen     = newLen + right.len
		newCenterLen = totalLen >> 1
		newRightLen  = totalLen - newCenterLen
		rightHead    = right.len - newRightLen
	)

	newCenter := newNode[T](newCenterLen, edit)
	newRight := newNode[T](newRightLen, edit)

	ks := keyStitcher[T]{newCenter.keys, 0}
	ks.copyAll(n.keys, 0, idx-1)
	if nodes[0] != nil {
		ks.copyOne(nodes[0].maxKey())
	}
	ks.copyOne(nodes[1].maxKey())
	if nodes[2] != nil {
		ks.copyOne(nodes[2].maxKey())
	}
	ks.copyAll(n.keys, idx+2, n.len)
	ks.copyAll(right.keys, 0, rightHead)

	copy(newRight.keys, right.keys[rightHead:right.len])

	cs := nodeStitcher[T]{newCenter.children, 0}
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
		nodes:  [3]node[T]{internalNodeToNode(left), newCenter, newRight},
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

func internalNodeToNode[T any](n *internalNode[T]) node[T] {
	if n != nil {
		return n
	}
	return nil
}
