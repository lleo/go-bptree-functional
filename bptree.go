/*
Package bptree implements a persistent B+Tree in go.
*/
package bptree

import (
	"fmt"
	"log"
	"os"
)

//BpTree implemntents all the User facing API for the B+Tree persistent
//implementation.
//
type BpTree interface {
	IsEmpty() bool
	Equals(BpTree) bool
	Order() int
	String() string
	NumberOfEntries() int
	Depth() int
	Get(BptKey) (interface{}, bool)
	//Modifying Ops
	Put(BptKey, interface{}) (BpTree, bool)
	Del(BptKey) (BpTree, interface{}, bool)
}

//BptKey is the interface the user must implement to create their own BptKey
//type. The only provided one is StringKey with my own interpretation of what
//less-than should mean for strings.
type BptKey interface {
	Equals(BptKey) bool
	LessThan(BptKey) bool
	String() string
}

type nodeI interface {
	String() string
	equals(nodeI) bool
	isToBig() bool
	isLeaf() bool
	findLeftMostKey() BptKey
	order() int
	size() int
	halfFullSize() int
}

var lgr = log.New(os.Stderr, "[bptree] ", log.Lshortfile)

type tree struct {
	root    nodeI
	order   int
	numEnts int
	depth   int
}

func mkTree(order int) *tree {
	var t = new(tree)
	t.root = mkLeaf(order)
	t.order = order
	t.numEnts = 0
	t.depth = 0
	return t
}

//creates a shallow copy of the old *tree structure returning a new *tree
//structure
func (ot *tree) copy() *tree {
	t := mkTree(ot.order)
	t.root = ot.root
	t.order = ot.order
	t.numEnts = ot.numEnts
	t.depth = ot.depth
	return t
}

func (t *tree) setRootNode(node *interiorNodeS) {
	t.root = node
	if len(node.keys) == 0 {
		t.depth--
		t.root = node.vals[0]
	}
}

func (t *tree) setRootLeaf(leaf *leafNodeS) {
	t.root = leaf
}

func (t *tree) newRootNode(k BptKey, l, r nodeI) {
	node := mkNode(t.order)
	node.keys = append(node.keys, k)
	node.vals = append(node.vals, l, r)

	t.root = node
	t.depth++
}

//NewBpTree instantiates a new B+Tree for a given order. The order controls
//the number of index keys and node children for inner tree nodes, and
//key/value pairs at the leaf node level. The general rule is that nodes
//fluxuate between about half full of entries, ceil(order/2), at the low end
//and order-1 entries when they are considered full.
//
//For this implementation we support order=3 and up. An order=32 B+Tree can
//cover 2 Billion (2GB) entries in at most 7 levels. That means scanning nodes
//between 16 and 31 entries 7 times to find any entry in the index. And
//practically that is more towards 16 than 31 and 6 times is more common than 7.
//
//The B+Tree order is a constant for the life of a B+Tree.
func NewBpTree(order int) BpTree {
	if order < 3 {
		lgr.Panic("Cannot make a BpTree with lessthan order=3")
	}
	return mkTree(order)
}

func (t *tree) IsEmpty() bool {
	var emptyByNumEnts bool
	if t.numEnts == 0 {
		emptyByNumEnts = true
	}
	var emptyByRootLeaf bool
	rootLeaf, ok := t.root.(*leafNodeS)
	if ok {
		assert(len(rootLeaf.keys) == len(rootLeaf.vals), "tree.IsEmpty: len(rootLeaf.keys) != len(rootLeaf.vals)")
		if len(rootLeaf.keys) == 0 {
			emptyByRootLeaf = true
		}
	}
	assert(emptyByNumEnts == emptyByRootLeaf, "emptyByNumEnts != emptyByRootLeaf")
	return emptyByNumEnts && emptyByRootLeaf
}

//Equals() does a Deep equivelence check between trees.
//
func (t *tree) Equals(other BpTree) bool {
	ot := other.(*tree)

	if t != ot {
		lgr.Println("Equals: t != ot; pointers are not equal;")
		return false
	}

	//lgr.Printf("t.Equals(ot) t,%p == ot,%p", t, ot)

	switch trn := t.root.(type) {
	case *leafNodeS:
		switch orn := ot.root.(type) {
		case *leafNodeS:
			if trn != orn {
				lgr.Printf("t.root and other.rooth are both *leafNodeS, but not the same pointer; t.root=%p, other.root=%p", trn, orn)
			} else {
				//TRACING PRINT
				//lgr.Printf("Equals: t.equals(tree.root, other.root, 0, 0) for calling t.root.(type),%T\n", trn)
				return t.equals(trn, orn, 0, 0)
			}
		case *interiorNodeS:
			lgr.Printf("t.root is *leafNodeS and other.root is *interiorNodeS")
		default:
			lgr.Printf("t.root is *leafNodeS and other.root is unknown=%T", orn)
		}
	case *interiorNodeS:
		switch orn := ot.root.(type) {
		case *leafNodeS:
			lgr.Printf("t.root is *interiorNodeS & other.root is *leafNodeS")
		case *interiorNodeS:
			if trn != orn {
				lgr.Printf("t.root and other.rooth are both *interiorNodeS, but not the same pointer; t.root=%p, other=%p", trn, orn)
			} else {
				//TRACING PRINT
				//lgr.Printf("Equals: calling t.equals(tree.root, other.root, 0, 0) for t.root.(type),%T;\n", trn)
				return t.equals(trn, orn, 0, 0)
			}
		default:
			lgr.Printf("t.root is *interiorNodeS and other is unknown=%T", orn)
		}
	default:
		lgr.Printf("t.root unknown type=%T\n", trn)
	}

	return false
}

func (t *tree) equals(tn, on nodeI, idx, depth int) bool {
	//TRACING PRINT
	//lgr.Printf("t.equals: idx=%d; depth=%d", idx, depth)
	if tn.isLeaf() {
		tnl := tn.(*leafNodeS)
		onl := on.(*leafNodeS)
		if len(tnl.keys) != len(onl.keys) {
			lgr.Printf("len(tnl.keys),%d != len(onl.keys),%d\n", len(tnl.keys), len(onl.keys))
			return false
		}
		if len(tnl.vals) != len(onl.vals) {
			lgr.Printf("len(tnl.vals),%d != len(onl.vals),%d\n", len(tnl.vals), len(onl.vals))
			return false
		}
		if len(tnl.keys) != len(tnl.vals) {
			lgr.Println("tnl & onl are both *leafNodeS")
			lgr.Printf("len(tnl.keys),%d != len(tnl.vals),%d\n", len(tnl.keys), len(tnl.vals))
			return false
		}
		for i := range tnl.keys {
			if !tnl.keys[i].Equals(onl.keys[i]) {
				lgr.Printf("tnl.keys[%d],%q != onl.keys[%d],%q\n", i, tnl.keys[i], i, onl.keys[i])
				return false
			}
		}
		for i := range tnl.vals {
			if tnl.vals[i] != onl.vals[i] {
				lgr.Printf("tnl.vals[%d],%v != onl.vals[%d],%v\n", i, tnl.vals[i], i, onl.vals[i])
				return false
			}
		}
		//TRACING PRINT
		//lgr.Printf("t.equals: tn.isLeaf: should return true")
	} else {
		tni := tn.(*interiorNodeS)
		oni := on.(*interiorNodeS)
		if len(tni.keys) != len(oni.keys) {
			lgr.Printf("len(tni.keys),%d != len(oni.keys),%d\n", len(tni.keys), len(oni.keys))
			return false
		}
		if len(tni.vals) != len(oni.vals) {
			lgr.Printf("len(tni.vals),%d != len(oni.vals),%d\n", len(tni.vals), len(oni.vals))
			return false
		}
		if len(tni.keys) != len(tni.vals)-1 {
			lgr.Println("tni && oni are both *interiorNodeS")
			lgr.Printf("len(tni.keys),%d != len(tni.vals)-1,%d\n", len(tni.keys), len(tni.vals)-1)
			return false
		}
		for i := range tni.vals {
			switch tnn := tni.vals[i].(type) {
			case *leafNodeS:
				switch onn := oni.vals[i].(type) {
				case *leafNodeS:
					if tnn != onn {
						lgr.Printf("tni.vals[%d] and oni.vals[%d] are both *leafNodeS, but not the same pointer; tni.vals[%d]=%p, oni.vals[%d]=%p", i, i, i, tnn, i, onn)
					} else {
						//TRACING PRINT
						//lgr.Printf("t.equals: *leafNodeS recursing i=%d depth+1=%d\n", i, depth+1)
						return t.equals(tnn, onn, i, depth+1)
					}
				case *interiorNodeS:
					lgr.Printf("tni.vals[%d] is *leafNodeS and oni.vals[%d] is *interiorNodeS", i, i)
				default:
					lgr.Printf("tni.vals[%d] is *leafNodeS and oni.vals[%d] is unknown=%T", i, i, onn)
				}
			case *interiorNodeS:
				switch onn := oni.vals[i].(type) {
				case *leafNodeS:
					lgr.Printf("tni.vals[%d] is *interiorNodeS and oni.vals[%d] is *leafNodeS", i, i)
				case *interiorNodeS:
					if tnn != onn {
						lgr.Printf("tni.vals[%d] and oni.vals[%d] are both *interiorNodeS, but not the same pointer; tni.vals[%d]=%p, oni.vals[%d]=%p", i, i, i, tnn, i, onn)
					} else {
						//TRACING PRINT
						//lgr.Printf("t.equals: *interiorNodeS recursing i=%d depth+1=%d\n", i, depth+1)
						return t.equals(tnn, onn, i, depth+1)
					}
				default:
					lgr.Printf("tni.vals[%d] is *interiorNodeS and oni.vals[%d] is unknown=%T", i, i, onn)
				}
			default:
				lgr.Printf("tni.vals[%d] unknown type=%T\n", i, tnn)
			}
		} //for i := range tni.vals
	} // else //!tn.isLeaf()

	//TRACING PRINT
	//lgr.Printf("t.equals: returning true; idx=%d; depth=%d", idx, depth)
	return true
} // func (t *tree) equals(...) boola

//Order returns the order of the *tree
//
func (t *tree) Order() int {
	return t.order
}

//String creates a string representation of the *tree structure.
//
func (t *tree) String() string {
	s := ""
	if t.root.isLeaf() {
		rootLeaf := t.root.(*leafNodeS)
		s += fmt.Sprintf("TREE: root=%p; order=%d;\n", rootLeaf, t.order)
		s += "\n"
		s += fmt.Sprint(rootLeaf)
	} else { // t.root is an interiorNodeS
		rootNode := t.root.(*interiorNodeS)

		s += fmt.Sprintf("TREE: root=%p; order=%d\n", rootNode, t.order)
		s += "\n"
		s += fmt.Sprint(rootNode)

		nodes := make([]nodeI, 0, 2)

		//seed the nodes slice
		for i := 0; i < len(rootNode.vals); i++ {
			nodes = append(nodes, rootNode.vals[i])
		}

		//printing nodes and conditionally adding new nodes
		for i := 0; i < len(nodes); i++ {
			s += fmt.Sprint(nodes[i])
			if !nodes[i].isLeaf() {
				tNode := nodes[i].(*interiorNodeS)
				nodes = append(nodes, tNode.vals...)
			}
		}
	}
	return s
}

//NumberOfEntries() returns the number of entries in the B+Tree.
//
func (t *tree) NumberOfEntries() int {
	return t.numEnts
}

//Depth() returns the depth of the tree
//
func (t *tree) Depth() int {
	return t.depth
}

//Get(key) returns the value stored for key, and a boolean that indicates
//if it was found or not.
//
func (t *tree) Get(key BptKey) (interface{}, bool) {
	//Find a Leaf matching BptKey from the root of *tree
	leaf, _ := t.findLeaf(key)

	for i, k := range leaf.keys {
		if key.Equals(k) {
			return leaf.vals[i], true
		}
	}

	return nil, false
}

//Put(key, val)
//
func (ot *tree) Put(key BptKey, val interface{}) (BpTree, bool) {
	t := ot.copy()

	oldLeaf, path := t.findLeaf(key)

	newLeaf := oldLeaf.copy()

	added := newLeaf.insert(key, val)
	if added {
		t.numEnts++
	}

	if newLeaf.isToBig() {
		rightLeaf, rightKey := newLeaf.split()
		t.insertUpLeaf(oldLeaf, newLeaf, rightKey, rightLeaf, path)
	} else {
		t.copyUpLeaf(oldLeaf, newLeaf, path)
	}

	return t, added
}

func (t *tree) insertUpLeaf(
	oldLeaf, newLeaf *leafNodeS,
	splitKey BptKey, splitLeaf *leafNodeS,
	path pathT,
) {
	if path.isEmpty() {
		t.newRootNode(splitKey, newLeaf, splitLeaf)
		return
	}

	oldParent := path.pop()
	newParent := oldParent.copy()

	newParent.swapLeafNode(oldLeaf, newLeaf)

	newParent.insert(splitKey, splitLeaf)

	if newParent.isToBig() {
		rightNode, rightKey := newParent.split()
		t.insertUp(oldParent, newParent, rightKey, rightNode, path)
	} else {
		t.copyUp(oldParent, newParent, path)
	}
}

func (t *tree) insertUp(
	oldNode, newNode *interiorNodeS,
	splitKey BptKey, splitNode *interiorNodeS,
	path pathT,
) {
	if path.isEmpty() {
		rootNode := t.root.(*interiorNodeS)
		assert(rootNode == oldNode, "rootNode != oldNode")

		t.newRootNode(splitKey, newNode, splitNode)
		return
	}

	oldParent := path.pop()
	newParent := oldParent.copy()

	newParent.swapInteriorNode(oldNode, newNode)

	newParent.insert(splitKey, splitNode)

	if newParent.isToBig() {
		rightNode, rightKey := newParent.split()
		t.insertUp(oldParent, newParent, rightKey, rightNode, path)
	} else {
		t.copyUp(oldParent, newParent, path)
	}
}

func (t *tree) copyUpLeaf(oldLeaf, newLeaf *leafNodeS, path pathT) {
	if path.isEmpty() {
		assert(t.isRoot(oldLeaf), "!t.isRoot(oldLeaf)")
		t.setRootLeaf(newLeaf)
		return
	}

	oldParent := path.pop()
	newParent := oldParent.copy()

	newParent.swapLeafNode(oldLeaf, newLeaf)

	t.copyUp(oldParent, newParent, path)
}

func (t *tree) copyUp(oldNode, newNode *interiorNodeS, path pathT) {
	if path.isEmpty() {
		assert(t.isRoot(oldNode), "path.isEmpty() && !t.isRoot(oldNode)")
		//WARNING: has additional check for shrinking the tree
		t.setRootNode(newNode)
		return
	}

	oldParent := path.pop()
	newParent := oldParent.copy()

	newParent.swapInteriorNode(oldNode, newNode)

	t.copyUp(oldParent, newParent, path)
}

func (ot *tree) Del(key BptKey) (BpTree, interface{}, bool) {
	t := ot.copy()

	oldLeaf, path := t.findLeaf(key)

	newLeaf := oldLeaf.copy()

	val, removed := newLeaf.remove(key)
	if !removed {
		return ot, val, removed
	}
	// removed == true && val == val removed from here on

	//keep t.numEnts up to date
	t.numEnts--

	if ot.isRoot(oldLeaf) {
		assert(path.isEmpty(), "ot.isroot(oldLeaf) && !path.isEmpty()")

		//reuse first part of copyUpLeaf
		t.copyUpLeaf(oldLeaf, newLeaf, path)

		return t, val, removed
	}
	//ELSE !path.isEmpty()

	if !newLeaf.isToSmall() { //aka newLeaf.size() >= newLeaf.halfFullSize()
		t.copyUpLeaf(oldLeaf, newLeaf, path)
		return t, val, removed
	}
	//ELSE newLeaf.isToSmall() from here after

	oldParent := path.pop()

	//findPeerLeft
	oldLeafLeft, leftKey := oldLeaf.findPeerLeft(oldParent)
	if oldLeafLeft != nil {
		if oldLeafLeft.size() > oldLeafLeft.halfFullSize() {
			newLeftLeaf := oldLeafLeft.copy()
			newLeaf.stealLeft(newLeftLeaf)

			newParent := oldParent.copy()

			newParent.swapKey(leftKey, newLeaf.findLeftMostKey())
			newParent.swapLeafNode(oldLeafLeft, newLeftLeaf)
			newParent.swapLeafNode(oldLeaf, newLeaf)

			t.copyUp(oldParent, newParent, path)

			return t, val, removed
		}
	}

	//findPeerRight
	oldLeafRight, rightKey := oldLeaf.findPeerRight(oldParent)
	if oldLeafRight != nil {
		if oldLeafRight.size() > oldLeafRight.halfFullSize() {
			newRightLeaf := oldLeafRight.copy()
			newLeaf.stealRight(newRightLeaf)

			newParent := oldParent.copy()

			newParent.swapKey(rightKey, newRightLeaf.findLeftMostKey())
			newParent.swapLeafNode(oldLeafRight, newRightLeaf)
			newParent.swapLeafNode(oldLeaf, newLeaf)

			t.copyUp(oldParent, newParent, path)

			return t, val, removed
		}
	}

	//merge
	var oldMergedLeaf *leafNodeS
	var newMergedLeaf *leafNodeS
	var deadLeaf *leafNodeS
	if oldLeafLeft == nil && oldLeafRight == nil {
		lgr.Panic("oldLeafLeft == nil && oldLeafRight == nil; should not be able to happend outside order==2 which we don't support.")
	}
	//else either or both leftLeaf&rightLeaf != nil
	if oldLeafLeft != nil {
		newLeafLeft := oldLeafLeft.copy()

		//newLeaf got sucked into newLeafLeft so it will be gc'd
		newLeafLeft.mergeRight(newLeaf)
		oldMergedLeaf = oldLeafLeft
		newMergedLeaf = newLeafLeft
		deadLeaf = oldLeaf
	} else if oldLeafRight != nil {
		//olRightLeaf is not modified so it doesn't need to be copied
		//to newRightLeaf
		newLeaf.mergeRight(oldLeafRight)
		oldMergedLeaf = oldLeaf
		newMergedLeaf = newLeaf
		deadLeaf = oldLeafRight
	}

	t.delUpLeaf(oldParent, oldMergedLeaf, newMergedLeaf, deadLeaf, path)

	return t, val, removed
}

func (t *tree) updateInteriorParent(
	oldParent *interiorNodeS,
	oldSwapKey, newSwapKey BptKey,
	oldStolenNode, newStolenNode,
	oldPrimaryNode, newPrimaryNode *interiorNodeS,
	path pathT,
) {
	newParent := oldParent.copy()

	newParent.swapKey(oldSwapKey, newSwapKey)

	newParent.swapInteriorNode(oldStolenNode, newStolenNode)
	newParent.swapInteriorNode(oldPrimaryNode, newStolenNode)

	t.copyUp(oldParent, newParent, path)
}

func (t *tree) delUpLeaf(
	oldParent *interiorNodeS,
	oldMergedLeaf, newMergedLeaf, deadLeaf *leafNodeS,
	path pathT,
) {
	newParent := oldParent.copy()

	//Replace oldMergedLeaf with newMergedLeaf
	newParent.swapLeafNode(oldMergedLeaf, newMergedLeaf)

	//Remove deadLeaf from newParent
	var i int
	origLen := len(newParent.vals)
	for i = 0; i < origLen; i++ {
		if deadLeaf.equals(newParent.vals[i]) {
			if i == 0 {
				lgr.Panic("delUpLeaf: oldMergeLeaf,%p was before deadLeaf,%p by definition in t.Del(); oldParent=\n%vnewParent=\n%v", oldMergedLeaf, deadLeaf, oldParent, newParent)
			}
			newParent.keys = append(newParent.keys[:i-1], newParent.keys[i:]...)
			newParent.vals = append(newParent.vals[:i], newParent.vals[i+1:]...)

			break //guaranteed i != orgLen
		}
	}
	if i == origLen {
		lgr.Panicf("delUpLeaf: i == len(newParent.vals); so we didn't find deadLeaf; THIS IS BAD!!! deadLeaf=%p; oldParent=\n%vnewParent=\n%v", deadLeaf, oldParent, newParent)
		//deadLeaf should have beein either oldLeaf or
		//oldRightLeaf (found by oldLeaf.findPeerRight())
	}

	//Did I just shrink the Root?
	if t.isRoot(oldParent) {
		assert(path.isEmpty(), "t.isRoot(oldParent) && !path.Empty")

		t.setRootNode(newParent)

		return
	}
	//ELSE !path.isEmpty()

	if !newParent.isToSmall() {
		//Well there is nothiing to do but copyUp the path
		t.copyUp(oldParent, newParent, path)
		return
	}
	//ELSE newParent.isToSmall() so we must fill it up with stealing or merging

	//The newParent isToSmall so:
	//  pop grandparent off path
	//  findPeerLeft with grandparent
	//  if found leftPeer
	//    if leftPeer is big enough to steal from
	//       steal from leftPeer
	//       update Grandparent
	//
	//   findPeerRight with grandparent
	//   if found rightPeer
	//     if rightPeer is big enough to steal from
	//       steal from rightPeer
	//       update grandparent
	//
	//   merge
	//   t.delUp(grandparent, oldmergednode, newmergednode, deadnode, path)

	oldGrandParent := path.pop()

	oldPeerLeft, leftKey := oldParent.findPeerLeft(oldGrandParent)
	if oldPeerLeft != nil {
		if oldPeerLeft.size() > oldPeerLeft.halfFullSize() {
			//leftPeer is big enough to steal from
			newPeerLeft := oldPeerLeft.copy()
			newParent.stealLeft(newPeerLeft)

			newGrandParent := oldGrandParent.copy()

			newGrandParent.swapKey(leftKey, newParent.findLeftMostKey())
			newGrandParent.swapInteriorNode(oldPeerLeft, newPeerLeft)
			newGrandParent.swapInteriorNode(oldParent, newParent)

			t.copyUp(oldGrandParent, newGrandParent, path)

			return
		}
	}

	oldPeerRight, rightKey := oldParent.findPeerRight(oldGrandParent)
	if oldPeerRight != nil {
		if oldPeerRight.size() > oldPeerRight.halfFullSize() {
			//rightPeer is big enough to steal from
			newPeerRight := oldPeerRight.copy()
			newParent.stealRight(newPeerRight)

			newGrandParent := oldGrandParent.copy()

			newGrandParent.swapKey(rightKey, newPeerRight.findLeftMostKey())
			newGrandParent.swapInteriorNode(oldPeerRight, newPeerRight)
			newGrandParent.swapInteriorNode(oldParent, newParent)

			t.copyUp(oldGrandParent, newGrandParent, path)

			return
		}
	}
	//ELSE either oldPeerLeft & oldPeerRight != nil

	//merge
	var oldMergedNode *interiorNodeS
	var newMergedNode *interiorNodeS
	var deadNode *interiorNodeS
	if oldPeerLeft == nil && oldPeerRight == nil {
		lgr.Panic("oldPeerLeft == nil && oldPeerRight == nil; should not be able to heppen outside order=2 which we don't support")
	}
	if oldPeerLeft != nil {
		newPeerLeft := oldPeerLeft.copy()
		newPeerLeft.mergeRight(newParent)
		oldMergedNode = oldPeerLeft
		newMergedNode = newPeerLeft
		deadNode = oldParent
	} else if oldPeerRight != nil {
		newParent.mergeRight(oldPeerRight)
		oldMergedNode = oldParent
		newMergedNode = newParent
		deadNode = oldPeerRight
	}

	//Did not use/modify oldGrandParent; so put it back
	//path.push(oldGrandParent)

	t.delUp(oldGrandParent, oldMergedNode, newMergedNode, deadNode, path)
}

func (t *tree) delUp(
	oldParent *interiorNodeS,
	oldMergedNode, newMergedNode, deadNode *interiorNodeS,
	path pathT,
) {
	newParent := oldParent.copy()

	//Replace oldMergedNode with newMergedNode
	newParent.swapInteriorNode(oldMergedNode, newMergedNode)

	//Remove the deadNode from the newParent
	var i int
	var origLen = len(newParent.vals)
	for i = 0; i < origLen; i++ {
		if deadNode.equals(newParent.vals[i]) {
			if i == 0 {
				lgr.Panic("delUp: oldMergedNode,%p was before deadNode,%p by definition in t.Del(); oldParent=\n%vnewParent=\n%v", oldMergedNode, deadNode, oldParent, newParent)
			}
			newParent.keys = append(newParent.keys[:i-1], newParent.keys[i:]...)
			newParent.vals = append(newParent.vals[:i], newParent.vals[i+1:]...)

			break
		}
	}
	if i == origLen {
		lgr.Panic("delUp: i == len(newParent.vals); so we didn't find deadNode; THIS IS BAD!!! deadNode=%p; oldParent=\n%vnewParent=\n%v", deadNode, oldParent, newParent)
		//deadNode should have beein either oldNode or
		//oldRightNode (found by oldNode.findPeerRight())
	}

	//Did I just shrink the Root?
	if t.isRoot(oldParent) {
		assert(path.isEmpty(), "t.isRoot(oldParent) && !path.Empty")

		t.setRootNode(newParent)

		return
	}
	//ELSE !path.isEmpty()

	if !newParent.isToSmall() {
		//Well there is nothing to do but copyUp the path
		t.copyUp(oldParent, newParent, path)
		return
	}
	//ELSE newParent.isToSmall() so we must fill it with stealing or merging

	//pop grandparent off path
	oldGrandParent := path.pop()

	oldPeerLeft, leftKey := oldParent.findPeerLeft(oldGrandParent)
	if oldPeerLeft != nil {
		if oldPeerLeft.size() > oldPeerLeft.halfFullSize() {
			//leftPeer is big enough to steal from
			newPeerLeft := oldPeerLeft.copy()
			newParent.stealLeft(newPeerLeft)

			newGrandParent := oldGrandParent.copy()

			newGrandParent.swapKey(leftKey, newParent.findLeftMostKey())
			newGrandParent.swapInteriorNode(oldPeerLeft, newPeerLeft)
			newGrandParent.swapInteriorNode(oldParent, newParent)

			t.copyUp(oldGrandParent, newGrandParent, path)

			return
		}
	}

	oldPeerRight, rightKey := oldParent.findPeerRight(oldGrandParent)
	if oldPeerRight != nil {
		if oldPeerRight.size() > oldPeerRight.halfFullSize() {
			//rightPeer is big enough to steal from
			newPeerRight := oldPeerRight.copy()
			newParent.stealRight(newPeerRight)

			newGrandParent := oldGrandParent.copy()

			newGrandParent.swapKey(rightKey, newPeerRight.findLeftMostKey())
			newGrandParent.swapInteriorNode(oldPeerRight, newPeerRight)
			newGrandParent.swapInteriorNode(oldParent, newParent)

			t.copyUp(oldGrandParent, newGrandParent, path)

			return
		}
	}
	//ELSE either oldPeerLeft & oldPeerRight != nil

	//merge
	//can not use oldMergedNode, newMergedNode, and deadNode variable names
	var oldMNode *interiorNodeS
	var newMNode *interiorNodeS
	var dNode *interiorNodeS
	if oldPeerLeft == nil && oldPeerRight == nil {
		lgr.Panic("oldPeerLeft == nil && oldPeerRight == nil; should not be able to heppen outside order=2 which we don't support")
	}
	if oldPeerLeft != nil {
		newPeerLeft := oldPeerLeft.copy()
		newPeerLeft.mergeRight(newParent)
		oldMNode = oldPeerLeft
		newMNode = newPeerLeft
		dNode = oldParent
	} else if oldPeerRight != nil {
		newParent.mergeRight(oldPeerRight)
		oldMNode = oldParent
		newMNode = newParent
		dNode = oldPeerRight
	}

	//Did not use/modify oldGrandParent; so put it back
	//path.push(oldGrandParent)

	t.delUp(oldGrandParent, oldMNode, newMNode, dNode, path)
}

func (t *tree) Graph() string {
	return ""
}

func (t *tree) isRoot(node nodeI) bool {
	return t.root.equals(node)
}

func (t *tree) findLeaf(key BptKey) (*leafNodeS, pathT) {
	path := newPathT()

	nextNode := t.root

	for !nextNode.isLeaf() {
		curNode := nextNode.(*interiorNodeS)

		path.push(curNode)
		var i int
		for i = 0; i < len(curNode.keys); i++ {
			if key.LessThan(curNode.keys[i]) {
				nextNode = curNode.vals[i]
				break // guaranteed i != len(curNode.keys)
			}
		}
		if i == len(curNode.keys) {
			nextNode = curNode.vals[i]
		}
	}

	return nextNode.(*leafNodeS), path
}

//func (t *tree) findLeftMostLeaf() (*leafNodeS, pathT) {
//	path := newPathT()
//
//	nextNode := t.root
//
//	for !nextNode.isLeaf() {
//		curNode := nextNode.(*interiorNodeS)
//		path.push(curNode)
//
//		nextNode = curNode.vals[0]
//	}
//
//	return nextNode.(*leafNodeS), path
//}

func (t *tree) findRightMostLeaf() (*leafNodeS, pathT) {
	path := newPathT()

	nextNode := t.root

	for !nextNode.isLeaf() {
		curNode := nextNode.(*interiorNodeS)
		path.push(curNode)

		nextNode = curNode.vals[len(curNode.vals)-1]
	}

	return nextNode.(*leafNodeS), path
}

func (t *tree) findLastKey() (BptKey, pathT) {
	leaf, path := t.findRightMostLeaf()

	lastKey := leaf.keys[len(leaf.keys)-1]

	return lastKey, path
}
