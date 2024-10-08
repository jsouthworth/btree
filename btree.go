// Package btree implements a persistent B+Tree
package btree

import (
	"cmp"
	"fmt"
	"iter"
	"strings"

	"jsouthworth.net/go/btree/internal/atomic"
)

type Error string

func (e Error) Error() string {
	return string(e)
}

const ErrTafterP = Error("transient used after persistent call")

type BTree[T any] struct {
	root    *node[T]
	count   int
	version int
	edit    *atomic.Bool

	cmp compareFunc[T]
	eq  eqFunc[T]
}

var emptyEdit = atomic.NewBool(false)

func Empty[T any](cmp func(a, b T) int, eq func(a, b T) bool) *BTree[T] {
	return &BTree[T]{
		root: newLeaf[T](0, emptyEdit).asNode(),
		edit: emptyEdit,
		cmp:  cmp,
		eq:   eq,
	}
}

func (t *BTree[T]) Contains(key T) bool {
	_, found := t.root.find(key, t.cmp)
	return found
}

func (t *BTree[T]) At(key T) T {
	out, _ := t.root.find(key, t.cmp)
	return out
}

func (t *BTree[T]) Find(key T) (T, bool) {
	return t.root.find(key, t.cmp)
}

func (t *BTree[T]) Add(key T) *BTree[T] {
	ret := t.root.add(key, t.cmp, t.eq, t.edit)
	var newRoot *node[T]
	switch ret.status {
	case returnUnchanged:
		return t
	case returnOne:
		newRoot = ret.nodes[0]
	case returnReplaced:
		return &BTree[T]{
			root:    ret.nodes[0],
			count:   t.count,
			version: t.version + 1,
			edit:    t.edit,
			cmp:     t.cmp,
			eq:      t.eq,
		}
	default:
		nr := newNode[T](2, t.edit)
		nr.keys[0] = ret.nodes[0].maxKey()
		nr.keys[1] = ret.nodes[1].maxKey()
		copy(nr.children, ret.nodes[:])
		newRoot = nr.asNode()
	}
	return &BTree[T]{
		root:    newRoot,
		count:   t.count + 1,
		version: t.version + 1,
		edit:    t.edit,
		cmp:     t.cmp,
		eq:      t.eq,
	}
}

func (t *BTree[T]) Delete(key T) *BTree[T] {
	ret := t.root.remove(key, nil, nil, t.cmp, t.edit)
	if ret.status == returnUnchanged {
		return t
	}
	newRoot := ret.nodes[1] // center
	if newRoot.isInternalNode() && newRoot.len == 1 {
		newRoot = newRoot.asInternalNode().children[0]
	}
	return &BTree[T]{
		root:    newRoot,
		count:   t.count - 1,
		version: t.version + 1,
		edit:    t.edit,
		cmp:     t.cmp,
		eq:      t.eq,
	}
}

func (t *BTree[T]) Length() int {
	return t.count
}

func (t *BTree[T]) String() string {
	var b strings.Builder
	t.root.string(&b, 1)
	return b.String()
}


// Iterator returns a stack allocated iterator. One may range over
// this using (Iterator[T]).Seq(). This can be useful to avoid
// allocations during iteration.
func (t *BTree[T]) Iterator() Iterator[T] {
	i := makeIterator(t.cmp, t.root)
	i.HasNext() // Make sure the initial iterator value is valid
	return i
}

// All allows one to range over the BTree. This will allocate memory
// on the heap for the iterator structure.
func (t *BTree[T]) All() iter.Seq[T] {
	i := t.Iterator()
	return i.Seq
}

// IteratorFrom allows one to start iterating with the first element
// greater than or equal to "from". One may range over this using
// (Iterator[T]).Seq. This can be useful to avoid allocations during
// iteration.
func (t *BTree[T]) IteratorFrom(from T) Iterator[T] {
	i := makeIterator(t.cmp, t.root)
	i.findFirst(from)
	i.HasNext() // Make sure the initial iterator value is valid
	return i
}

// From allows one to range over the BTree starting with the first
// element greater than or equal to "from". This will allocate memory
// on the heap for the iterator structure.
func (t *BTree[T]) From(from T) iter.Seq[T] {
	i := t.IteratorFrom(from)
	return i.Seq
}

type Iterator[T any] struct {
	cmp   compareFunc[T]
	depth int
	stack [maxIterDepth]struct {
		n   *node[T]
		cur int8
	}
}

func (i *Iterator[T]) Seq(yield func(T) bool) {
	for i.HasNext() {
		if !yield(i.Next()) {
			break;
		}
	}
}

func makeIterator[T any](cmp compareFunc[T], n *node[T]) Iterator[T] {
	var i Iterator[T]
	i.cmp = cmp
	i.stack[0].n = n
	return i
}

func (i *Iterator[T]) Next() T {
	state := i.stack[i.depth]
	n := state.n.asLeafNode()
	out := n.keys[state.cur]
	i.stack[i.depth].cur++
	return out
}

func (i *Iterator[T]) HasNext() bool {
	state := i.stack[i.depth]
	switch state.n.kind {
	case nodeKindLeaf:
		n := state.n.asLeafNode()
		if state.cur < n.len {
			return true
		}
		if i.depth == 0 {
			return false
		}
		i.popNode()
		return i.HasNext()
	case nodeKindInternal:
		n := state.n.asInternalNode()
		if state.cur < n.len {
			child := n.children[state.cur]
			i.stack[i.depth].cur++
			i.pushNode(child)
			switch child.kind {
			case nodeKindLeaf:
				return true
			case nodeKindInternal:
				return i.HasNext()
			}
		}
		if i.depth == 0 {
			return false
		}
		i.popNode()
		return i.HasNext()
	default:
		return false
	}
}

func (i *Iterator[T]) pushNode(n *node[T]) {
	i.depth = i.depth + 1
	state := i.stack[i.depth]
	state.n = n
	state.cur = 0
	i.stack[i.depth] = state
}

func (i *Iterator[T]) popNode() {
	state := i.stack[i.depth]
	state.n = nil
	state.cur = 0
	i.stack[i.depth] = state
	i.depth = i.depth - 1
}

func (i *Iterator[T]) findFirst(from T) {
	for {
		state := i.stack[i.depth]
		switch state.n.kind {
		case nodeKindLeaf:
			n := state.n.asLeafNode()
			first := n.searchFirst(from, i.cmp)
			i.stack[i.depth].cur = first
			return
		case nodeKindInternal:
			n := state.n.asInternalNode()
			first := n.searchFirst(from, i.cmp)
			if first >= n.sizeOfChildArray() {
				i.stack[i.depth].cur = n.sizeOfChildArray()
				return
			}
			child := n.children[first]
			i.stack[i.depth].cur = first + 1
			i.pushNode(child)
		}
	}
}

type TBTree[T any] struct {
	root    *node[T]
	count   int
	version int
	edit    *atomic.Bool

	cmp compareFunc[T]
	eq  eqFunc[T]

	orig *BTree[T]
}

func (t *BTree[T]) AsTransient() *TBTree[T] {
	return &TBTree[T]{
		root:    t.root,
		count:   t.count,
		version: t.version,
		edit:    atomic.NewBool(true),
		cmp:     t.cmp,
		eq:      t.eq,

		orig: t,
	}
}

func (t *TBTree[T]) Contains(key T) bool {
	t.ensureEditable()
	_, found := t.root.find(key, t.cmp)
	return found
}

func (t *TBTree[T]) At(key T) T {
	t.ensureEditable()
	out, _ := t.root.find(key, t.cmp)
	return out
}

func (t *TBTree[T]) Find(key T) (T, bool) {
	t.ensureEditable()
	return t.root.find(key, t.cmp)
}

func (t *TBTree[T]) Add(key T) *TBTree[T] {
	t.ensureEditable()
	ret := t.root.add(key, t.cmp, t.eq, t.edit)
	switch ret.status {
	case returnUnchanged:
		return t
	case returnEarly:
	case returnReplaced:
		t.root = ret.nodes[0]
		t.version++
		return t
	case returnOne:
		t.root = ret.nodes[0]
	default:
		nr := newNode[T](2, t.edit)
		nr.keys[0] = ret.nodes[0].maxKey()
		nr.keys[1] = ret.nodes[1].maxKey()
		copy(nr.children, ret.nodes[:])
		t.root = nr.asNode()
	}
	t.count++
	t.version++
	return t
}

func (t *TBTree[T]) Delete(key T) *TBTree[T] {
	t.ensureEditable()
	ret := t.root.remove(key, nil, nil, t.cmp, t.edit)
	switch ret.status {
	case returnUnchanged:
		return t
	case returnEarly:
	default:
		newRoot := ret.nodes[1] // center
		if newRoot.isInternalNode() && newRoot.len == 1 {
			nr := newRoot.asInternalNode()
			newRoot = nr.children[0]
		}
		t.root = newRoot
	}
	t.count--
	t.version++
	return t
}

// Iterator returns a stack allocated iterator. One may range over
// this using (Iterator[T]).Seq. This can be useful to avoid
// allocations during iteration.
func (t *TBTree[T]) Iterator() Iterator[T] {
	t.ensureEditable()
	i := makeIterator(t.cmp, t.root)
	i.HasNext() // Make sure the initial iterator value is valid
	return i
}

// All allows one to range over the BTree. This will allocate memory
// on the heap for the iterator structure.
func (t *TBTree[T]) All() iter.Seq[T] {
	t.ensureEditable()
	i := t.Iterator()
	return i.Seq
}

// IteratorFrom allows one to start iterating with the first element
// greater than or equal to "from". One may range over this using
// (Iterator[T]).Seq. This can be useful to avoid allocations during
// iteration.
func (t *TBTree[T]) IteratorFrom(from T) Iterator[T] {
	t.ensureEditable()
	i := makeIterator(t.cmp, t.root)
	i.findFirst(from)
	i.HasNext() // Make sure the initial iterator value is valid
	return i
}

// From allows one to range over the BTree starting with the first
// element greater than or equal to "from". This will allocate memory
// on the heap for the iterator structure.
func (t *TBTree[T]) From(from T) iter.Seq[T] {
	t.ensureEditable()
	i := t.IteratorFrom(from)
	return i.Seq
}

func (t *TBTree[T]) Length() int {
	t.ensureEditable()
	return t.count
}

func (t *TBTree[T]) String() string {
	var b strings.Builder
	t.root.string(&b, 1)
	return b.String()
}

func (t *TBTree[T]) AsPersistent() *BTree[T] {
	t.ensureEditable()
	t.edit.Reset(false)
	if t.root == t.orig.root {
		return t.orig
	}
	return &BTree[T]{
		root:    t.root,
		count:   t.count,
		version: t.version,
		edit:    t.edit,
		cmp:     t.cmp,
		eq:      t.eq,
	}
}

func (t *TBTree[T]) ensureEditable() {
	if !t.edit.Deref() {
		panic(ErrTafterP)
	}
}

type compareFunc[T any] func(k1, k2 T) int
type eqFunc[T any] func(k1, k2 T) bool

const (
	maxLen    = 64
	minLen    = maxLen >> 1
	expandLen = 8
	// maxIterDepth is log_32(^uintptr(0)) rounded up -- 13.
	// The height is calculated as h <= log_32((n+1)/2). The
	// maximum height must therefore be smaller than
	// log_32(^uintptr(0)) rounded up to the next value. To
	// calculate this we use log_2(^uintptr(0))/log_2(32). Which
	// is of course 64/5 = 12.8.  We round 64 up to get an even
	// 13.
	maxIterDepth = (64 + 1) / 5
)

type returnStatus uint8

const (
	returnUnchanged returnStatus = iota
	returnEarly
	returnReplaced
	returnOne
	returnTwo
	returnThree
)

var returnStatusStrings = [...]string{
	returnUnchanged: "unchanged",
	returnEarly:     "early",
	returnReplaced:  "replaced",
	returnOne:       "one",
	returnTwo:       "two",
	returnThree:     "three",
}

func (s returnStatus) String() string {
	return returnStatusStrings[s]
}

type nodeReturn[T any] struct {
	status returnStatus
	nodes  [3]*node[T]
}

func (r nodeReturn[T]) String() string {
	return fmt.Sprintf("{ %s %v %v %v }",
		r.status, r.nodes[0], r.nodes[1], r.nodes[2])
}

func min[T cmp.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T cmp.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
