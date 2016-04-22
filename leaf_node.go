package bptree

import (
	"fmt"
)

type leafNodeS struct {
	keys []BptKey
	vals []interface{}
}

func mkLeaf(order int) *leafNodeS {
	//set the capacity of keys and vals one slot to much
	//so the leaf can reach the isToBig() condition and be split
	var node = new(leafNodeS)
	node.keys = make([]BptKey, 0, order)
	node.vals = make([]interface{}, 0, order)
	return node
}

func (node *leafNodeS) copy() *leafNodeS {
	copyNode := mkLeaf(node.order())
	copyNode.keys = append(copyNode.keys, node.keys...)
	copyNode.vals = append(copyNode.vals, node.vals...)
	return copyNode
}

func (node *leafNodeS) String() string {
	s := ""
	s += fmt.Sprintf("%p: LEAF: len(node.keys)=%d; cap(node.keys)=%d; len(node.vals)=%d; cap(node.vals)=%d;\n", node, len(node.keys), cap(node.keys), len(node.vals), cap(node.vals))
	s += fmt.Sprintf("%p: keys = ", node)
	keys := make([]string, 0, 2)
	for _, key := range node.keys {
		keys = append(keys, fmt.Sprintf("%q", key.String()))
	}
	s += fmt.Sprintf("%v\n", keys)

	s += fmt.Sprintf("%p: vals = ", node)
	vals := make([]string, 0, 2)
	for _, v := range node.vals {
		vals = append(vals, fmt.Sprintf("%v", v))
	}
	s += fmt.Sprintf("%v\n", vals)
	s += "\n"
	return s
}

func (l *leafNodeS) equals(rn nodeI) bool {
	r := rn.(*leafNodeS) //blows up if casting doesn't work
	return l == r        //pointers are equal
}

//leaf.insert(key, val) returns true if a new key,val pair was inserted.
//leaf.insert(key, val) returns false if the val for a existing key,val pair
//was updated in place.
func (leaf *leafNodeS) insert(key BptKey, val interface{}) bool {
	var i int
	for i = 0; i < len(leaf.keys); i++ {
		switch {
		case key.Equals(leaf.keys[i]):
			leaf.vals[i] = val
			return false //replaced not inserted
		case key.LessThan(leaf.keys[i]):
			leaf.keys = append(leaf.keys[:i+1], leaf.keys[i:]...)
			leaf.vals = append(leaf.vals[:i+1], leaf.vals[i:]...)
			leaf.keys[i] = key
			leaf.vals[i] = val
			return true
		}
	}
	if i == len(leaf.keys) {
		leaf.keys = append(leaf.keys, key)
		leaf.vals = append(leaf.vals, val)
	}
	return true
}

func (leaf *leafNodeS) remove(key BptKey) (val interface{}, removed bool) {
	for i, k := range leaf.keys {
		if key.Equals(k) {
			val = leaf.vals[i]
			leaf.keys = append(leaf.keys[:i], leaf.keys[i+1:]...)
			leaf.vals = append(leaf.vals[:i], leaf.vals[i+1:]...)
			removed = true
			return
		}
	}
	return
}

//isToBig() was isFull, but that was a misnomer I go from the wikipedia post
//on B+Trees(https://en.wikipedia.org/wiki/B%2B_tree). In order for the FULL
//condition, AND maintain the node/leaf conditions spelled out in a table on
//that same page, then nodes/leafs must be allowed to get bigger than the
//order allows and thus qualify for the SPLIT operation. That condition
//should be TOBIG not FULL.
func (n *leafNodeS) isToBig() bool {
	return len(n.keys) == cap(n.keys)
}

func (n *leafNodeS) isToSmall() bool {
	return n.size() < n.halfFullSize()
}

//leafSplit must chop the receiving and overlarge(by one) leaf node in half.
//Leaving the original node shrunk by half and returning the new right half
//and the MIDDLE Key (of the orignial overlarge leaf node).
func (lNode *leafNodeS) split() (*leafNodeS, BptKey) {
	order := lNode.order()
	rLeaf := mkLeaf(order)

	//leafSplit for ODD orders makes the right node the larger node.
	//hence the MIDDLE KEY is rLeaf.keys[0], for ODD and EVEN orders.
	keySplitIdx := len(lNode.keys) / 2
	valSplitIdx := len(lNode.vals) / 2

	rLeaf.keys = append(rLeaf.keys, lNode.keys[keySplitIdx:]...)
	rLeaf.vals = append(rLeaf.vals, lNode.vals[valSplitIdx:]...)

	lNode.keys = append(lNode.keys[:0], lNode.keys[:keySplitIdx]...)
	lNode.vals = append(lNode.vals[:0], lNode.vals[:valSplitIdx]...)

	return rLeaf, rLeaf.keys[0]
}

func (rNode *leafNodeS) findPeerLeft(parent *interiorNodeS) (*leafNodeS, BptKey) {
	var leftPeerLeaf *leafNodeS
	var leftPeerKey BptKey
	var i int
	for i = 0; i < len(parent.vals); i++ {
		if rNode.equals(parent.vals[i]) {
			if i == 0 {
				//there is no left peer
				return nil, nil
			}
			leftPeerLeaf = parent.vals[i-1].(*leafNodeS)
			leftPeerKey = parent.keys[i-1]
			return leftPeerLeaf, leftPeerKey
		}
	}
	lgr.Panic("findPeerLeft: didn't find rNode(receiver) in parent")
	return nil, nil
}

func (lNode *leafNodeS) findPeerRight(parent *interiorNodeS) (*leafNodeS, BptKey) {
	var rightPeerLeaf *leafNodeS
	var rightPeerKey BptKey
	var i int
	for i = 0; i < len(parent.vals); i++ {
		if lNode.equals(parent.vals[i]) {
			if i == len(parent.vals)-1 {
				//there is no right peer
				return nil, nil
			}
			rightPeerLeaf = parent.vals[i+1].(*leafNodeS)
			rightPeerKey = parent.keys[i]
			return rightPeerLeaf, rightPeerKey
		}
	}
	lgr.Panic("findPeerRight: didn't find lNode(receiver) in parent")
	return nil, nil
}

//Given left peer, steal its right most
func (rLeaf *leafNodeS) stealLeft(lLeaf *leafNodeS) {
	stolenKey := lLeaf.keys[len(lLeaf.keys)-1]
	stolenVal := lLeaf.vals[len(lLeaf.vals)-1]
	//this preserves cap(lLeaf.keys) and cap(lLeaf.vals)
	lLeaf.keys = lLeaf.keys[:len(lLeaf.keys)-1]
	lLeaf.vals = lLeaf.vals[:len(lLeaf.vals)-1]

	//unshift operation that preserves cap(rLeaf.keys)
	rLeaf.keys = append(rLeaf.keys[:0],
		append([]BptKey{stolenKey}, rLeaf.keys...)...)
	//unshift operation that preserves cap(rLeaf.vals)
	rLeaf.vals = append(rLeaf.vals[:0],
		append([]interface{}{stolenVal}, rLeaf.vals...)...)
}

//Given right peer, steal its left most entry.
func (lLeaf *leafNodeS) stealRight(rLeaf *leafNodeS) {
	stolenKey := rLeaf.keys[0]
	stolenVal := rLeaf.vals[0]

	//this preserves cap(rLeaf.keys) and cap(rLeaf.vals)
	rLeaf.keys = append(rLeaf.keys[:0], rLeaf.keys[1:]...)
	rLeaf.vals = append(rLeaf.vals[:0], rLeaf.vals[1:]...)

	lLeaf.keys = append(lLeaf.keys, stolenKey)
	lLeaf.vals = append(lLeaf.vals, stolenVal)
}

func (lLeaf *leafNodeS) mergeRight(rLeaf *leafNodeS) {
	lLeaf.keys = append(lLeaf.keys, rLeaf.keys...)
	lLeaf.vals = append(lLeaf.vals, rLeaf.vals...)
}

func (n *leafNodeS) isLeaf() bool {
	return true
	//return cap(n.keys) == cap(n.vals)
}

func (leaf *leafNodeS) findLeftMostKey() BptKey {
	return leaf.keys[0]
}

func (n *leafNodeS) order() int {
	//in both leaf and interior nodes; see mkLeaf && mkNode
	return cap(n.keys)
}

func (n *leafNodeS) size() int {
	return len(n.vals)
}

func (n *leafNodeS) halfFullSize() int {
	// int(math.Ceil(float64(n)/2)) == (n+1)/2 (in integer math)

	//For leaf nodes halfFullSize == math.Ceil( (float64(order)-1)/2 )
	// cap(vals) == order (see func mkLeaf())
	// ceil((fload64(cap(vals))-1)/2) == (cap(vals)-1+1)/2 == cap(vals)/2

	return cap(n.vals) / 2
}
