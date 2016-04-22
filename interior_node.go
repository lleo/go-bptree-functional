package bptree

import (
	"fmt"
)

type interiorNodeS struct {
	keys []BptKey
	vals []nodeI
}

func mkNode(order int) *interiorNodeS {
	//set the capacity of keys and vals one slot to much
	//so the node can reach the isToBig() condition and be split
	var node = new(interiorNodeS)
	node.keys = make([]BptKey, 0, order)
	node.vals = make([]nodeI, 0, order+1)
	return node
}

func (node *interiorNodeS) copy() *interiorNodeS {
	copyNode := mkNode(node.order())
	copyNode.keys = append(copyNode.keys, node.keys...)
	copyNode.vals = append(copyNode.vals, node.vals...)
	return copyNode
}

func (node *interiorNodeS) swapLeafNode(oldLeaf, newLeaf *leafNodeS) {
	for i, n := range node.vals {
		ln := n.(*leafNodeS)
		if oldLeaf == ln {
			node.vals[i] = newLeaf
			return
		}
	}
	lgr.Panicf("swapLeafNode: did not find oldLeaf=%p to swap for newLeaf=%p; node=\n%v", oldLeaf, newLeaf, node)
}

func (node *interiorNodeS) swapInteriorNode(oldNode, newNode *interiorNodeS) {
	for i, n := range node.vals {
		ln := n.(*interiorNodeS)
		if oldNode == ln {
			node.vals[i] = newNode
			return
		}
	}
	lgr.Panicf("swapNodeNode: did not find oldNode=%p to swap for newNode=%p; node=\n%v", oldNode, newNode, node)
}

func (node *interiorNodeS) swapNode(oldNode, newNode nodeI) {
	if oldNode.isLeaf() {
		oleaf := oldNode.(*leafNodeS)
		nleaf := newNode.(*leafNodeS) //let it panic on failed casting

		for i, n := range node.vals {
			nl := n.(*leafNodeS) //another panic on failed casting
			if oleaf == nl {
				node.vals[i] = nodeI(nleaf)
				return
			}
		}
		lgr.Panicf("swapNode: did not find oleaf=%p to swap for nleaf=%p; node=\n%v", oleaf, nleaf, node)
	} else {
		onode := oldNode.(*interiorNodeS)
		nnode := newNode.(*interiorNodeS) //let it panic on failed casting

		for i, n := range node.vals {
			nn := n.(*interiorNodeS) //another panic on failed casting

			if onode == nn {
				node.vals[i] = nodeI(nnode)
				return
			}
		}
		lgr.Panicf("swapNode: did not find onode=%p to swap for nnode=%p; node=\n%v", onode, nnode, node)
	}

	return
}

func (node *interiorNodeS) swapKey(oldKey, newKey BptKey) {
	for i, k := range node.keys {
		if oldKey.Equals(k) {
			node.keys[i] = newKey
			return
		}
	}
	lgr.Panicf("swapKey: did not find oldKey=%q to swap for newKey=%q; node=\n%v", oldKey, newKey, node)
}

func (node *interiorNodeS) String() string {
	s := ""
	s += fmt.Sprintf("%p: NODE: len(node.keys)=%d; cap(node.keys)=%d; len(node.vals)=%d; cap(node.vals)=%d;\n", node, len(node.keys), cap(node.keys), len(node.vals), cap(node.vals))
	s += fmt.Sprintf("%p: keys = ", node)
	keys := make([]string, 0, 2)
	for _, key := range node.keys {
		keys = append(keys, fmt.Sprintf("%q", key.String()))
	}
	s += fmt.Sprintf("%v\n", keys)

	s += fmt.Sprintf("%p: vals = ", node)
	vals := make([]string, 0, 2)
	for _, v := range node.vals {
		//redundent; But hey, Type Safety! WooHoo! NOT!!!
		if v.isLeaf() {
			nv := v.(*leafNodeS)
			vals = append(vals, fmt.Sprintf("%p", nv))
		} else {
			nv := v.(*interiorNodeS)
			vals = append(vals, fmt.Sprintf("%p", nv))
		}
	}
	s += fmt.Sprintf("%v\n", vals)
	s += "\n"
	return s
}

func (l *interiorNodeS) equals(rn nodeI) bool {
	r, ok := rn.(*interiorNodeS)
	if !ok {
		return false //not the same type; clearly not equal.
	}
	return l == r //pointers are equal
}

//Only called after a val splits. So new key, val pair will be a new half of
//one of the vals.
func (node *interiorNodeS) insert(key BptKey, val nodeI) {
	//The only relation between node.keys[i] and node.vals[i] is that
	//node.keys[i] is strictly greater than any key in or below node.vals[i].
	//
	//The relationship between key and val passed in is that of node.keys[i]
	//to node.vals[i+1] where key may be found in val (or its decendents),
	//So we insert key into node.keys[i] and val into node.val[i+1].
	//
	var i int
	for i = 0; i < len(node.keys); i++ {
		if key.LessThan(node.keys[i]) {
			node.keys = append(node.keys[:i+1], node.keys[i:]...)
			//For interior nodes len(node.keys) == len(node.vals)-1 holds,
			//so this can not produce a "index out of range" error.
			node.vals = append(node.vals[:i+2], node.vals[i+1:]...)
			node.keys[i] = key
			node.vals[i+1] = val
			return
		}
	}
	//must have been the last val that split so this is valid because the
	//new key is greater than the last val and less than or equal to the
	//new val inserted.
	//if i == len(node.keys) {
	node.keys = append(node.keys, key)
	node.vals = append(node.vals, val)
	return
	//}
}

// isToBig() was isFull, but that was a misnomer I got from the wikipedia post
// on B+Trees(https://en.wikipedia.org/wiki/B%2B_tree). In order for the FULL
// condition, AND maintain the node/leaf conditions spelled out in a table on
// that same page, then nodes/leafs must be allowed to get bigger than the
// order allows and thus qualify for the SPLIT operation. That condition
// should be TOBIG not FULL.
func (node *interiorNodeS) isToBig() bool {
	return len(node.keys) == cap(node.keys)
}

func (n *interiorNodeS) isToSmall() bool {
	return n.size() < n.halfFullSize()
}

// nodeSplit chop the receiving and overlarge (by one) interior node in half.
// Leaving the original node shrunk by half and returning the new right half
// (minus the MIDDLE key) and the MIDDLE key (of the original overlarge
// node).
func (lNode *interiorNodeS) split() (*interiorNodeS, BptKey) {
	order := lNode.order()
	rNode := mkNode(order)

	//keySplitIdx := len(lNode.keys) / 2 //len(lNode.keys) == order
	//valSplitIdx := len(lNode.vals) / 2 //len(lNode.vals) == order+1
	keySplitIdx := order / 2
	valSplitIdx := (order + 1) / 2

	var midKey BptKey
	//if len(lNode.keys)%2 == 1 {
	if order%2 == 1 {
		//order is ODD eg 3, 5, 7 etc
		midKey = lNode.keys[keySplitIdx]

		rNode.keys = append(rNode.keys, lNode.keys[keySplitIdx+1:]...)
		rNode.vals = append(rNode.vals, lNode.vals[valSplitIdx:]...)

		//preserve the cap(lNode.keys) and cap(lNode.vals)
		lNode.keys = append(lNode.keys[:0], lNode.keys[:keySplitIdx]...)
		lNode.vals = append(lNode.vals[:0], lNode.vals[:valSplitIdx]...)
	} else {
		//order is EVEN eg 4, 6, 8 etc
		//the right side is fatter
		midKey = lNode.keys[keySplitIdx-1]

		rNode.keys = append(rNode.keys, lNode.keys[keySplitIdx:]...)
		rNode.vals = append(rNode.vals, lNode.vals[valSplitIdx:]...)

		//preserve the cap(lNode.keys) and cap(lNode.vals)
		lNode.keys = append(lNode.keys[:0], lNode.keys[:keySplitIdx-1]...)
		lNode.vals = append(lNode.vals[:0], lNode.vals[:valSplitIdx]...)
	}

	//*** Finding the middle Key ***
	//Remember: the node is "to big" aka one key larger than it should be
	//so we have called nodeSplit
	//For order is EVEN
	//  there are order(EVEN) number of keys
	//  the MIDDLE key could be either keySplitIdx or keySplitIdx-1
	//For order is ODD
	//  there are order(ODD) number of keys
	//  keySplitIdx is the Ceil of an odd number / 2
	//  midKey must come from the left node.keys cuz it has more
	//  the MIDDLE key is lNode.keys[keySplitIdx-1]

	return rNode, midKey
}

func (rNode *interiorNodeS) findPeerLeft(parent *interiorNodeS) (*interiorNodeS, BptKey) {
	var leftPeerNode *interiorNodeS
	var leftPeerKey BptKey
	var i int
	for i = 0; i < len(parent.vals); i++ {
		if rNode.equals(parent.vals[i]) {
			if i == 0 {
				//there is no left peer
				return nil, nil
			}
			leftPeerNode = parent.vals[i-1].(*interiorNodeS)
			leftPeerKey = parent.keys[i-1]
			return leftPeerNode, leftPeerKey
		}
	}
	lgr.Panic("findPeerLeft: didn't find rNode(receiver) in parent")
	return nil, nil
}

func (lNode *interiorNodeS) findPeerRight(parent *interiorNodeS) (*interiorNodeS, BptKey) {
	var rightPeerNode *interiorNodeS
	var rightPeerKey BptKey
	var i int
	for i = 0; i < len(parent.vals); i++ {
		if lNode.equals(parent.vals[i]) {
			if i == len(parent.vals)-1 {
				//there is no right peer
				return nil, nil
			}
			rightPeerNode = parent.vals[i+1].(*interiorNodeS)
			rightPeerKey = parent.keys[i]
			return rightPeerNode, rightPeerKey
		}
	}
	lgr.Panic("findPeerRight: didn't find lNode(receiver) in parent")
	return nil, nil
}

func (rNode *interiorNodeS) stealLeft(lNode *interiorNodeS) {
	//stolenKey := lNode.keys[len(lNode.keys)-1]
	stolenVal := lNode.vals[len(lNode.vals)-1]
	//this preserves cap(lNode.keys) and cap(lNode.vals)
	lNode.keys = append(lNode.keys[:0], lNode.keys[:len(lNode.keys)-1]...)
	lNode.vals = append(lNode.vals[:0], lNode.vals[:len(lNode.vals)-1]...)

	//Before modifying rNode what was its leastKey
	leastKey := rNode.findLeftMostKey()

	//Now we will insert leastKey as the first key in rNode, because
	//it is guaranteed to be greater-than anything in the stolen val/node.

	//unshift operation that preserves cap(rNode.vals)
	rNode.vals = append(rNode.vals[:0],
		append([]nodeI{stolenVal}, rNode.vals...)...)

	//unshift operation that preserves cap(rNode.keys)
	rNode.keys = append(rNode.keys[:0],
		append([]BptKey{leastKey}, rNode.keys...)...)

	return
}

func (lNode *interiorNodeS) stealRight(rNode *interiorNodeS) {
	//stolenKey := rNode.keys[0]
	stolenNode := rNode.vals[0]

	//this preserves cap(rNode.keys) and cap(rNode.vals)
	rNode.keys = append(rNode.keys[:0], rNode.keys[1:]...)
	rNode.vals = append(rNode.vals[:0], rNode.vals[1:]...)

	leastKey := stolenNode.findLeftMostKey()

	lNode.keys = append(lNode.keys, leastKey)
	lNode.vals = append(lNode.vals, stolenNode)

	return
}

func (lNode *interiorNodeS) mergeRight(rNode *interiorNodeS) {
	leastKey := rNode.findLeftMostKey()

	//For some reason you can't do the following append(...)
	//   lNode.keys = append(lNode.keys, leastKey, rNode.keys...)
	//you get "too many arguments to append"
	lNode.keys = append(lNode.keys, leastKey)
	lNode.keys = append(lNode.keys, rNode.keys...)
	lNode.vals = append(lNode.vals, rNode.vals...)

	return
}

func (node *interiorNodeS) isLeaf() bool {
	//FIXME: should remove these assertf() calls
	assertf(cap(node.keys)+1 == cap(node.vals), "cap(node.keys)+1,%d != cap(node.vals),%d", cap(node.keys)+1, cap(node.vals))
	assertf(len(node.keys)+1 == len(node.vals), "len(node.keys)+1,%d != len(node.vals),%d", len(node.keys)+1, len(node.vals))
	return false
}

func (node_ *interiorNodeS) findLeftMostLeaf() *leafNodeS {
	node := nodeI(node_)
	for !node.isLeaf() {
		node_ = node.(*interiorNodeS)
		node = node_.vals[0]
	}
	return node.(*leafNodeS)
}

func (node_ *interiorNodeS) findLeftMostKey() BptKey {
	//node := nodeI(node_)
	//for !node.isLeaf() {
	//	node_ = node.(*interiorNodeS)
	//	node = node_.vals[0]
	//}
	//return node.findLeftMostKey()
	leaf := node_.findLeftMostLeaf()
	return leaf.findLeftMostKey()
}

func (node *interiorNodeS) order() int {
	//in both leaf and interior nodes; see mkLeaf && mkNode
	return cap(node.keys)
}

func (node *interiorNodeS) size() int {
	return len(node.vals)
}

func (node *interiorNodeS) halfFullSize() int {
	// int(math.Ceil(float64(order)/2)) == (order+1)/2 (in integer math)

	//For interior nodes halfFullSize == math.Ceil( float64(order)/2 )
	// cap(vals) == order + 1 (see func mkNode())
	// ceil(float64(order)/2) == (order+1)/2 == cap(vals)/2

	return cap(node.vals) / 2
}
