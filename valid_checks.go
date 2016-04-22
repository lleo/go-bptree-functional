package bptree

import (
	"math"
)

func validTree(t *tree) bool {
	if !validRootNode(t.root, t.order) {
		return false
	}
	if t.root.isLeaf() {
		return true //else validRootNode(t.root) would have caught it
	}

	rootNode := t.root.(*interiorNodeS)

	nodes := make([]nodeI, 0, 2)

	//seed the nodes slice
	for i := 0; i < len(rootNode.vals); i++ {
		nodes = append(nodes, rootNode.vals[i])
	}

	for i := 0; i < len(nodes); i++ {
		if nodes[i].isLeaf() {
			node := nodes[i].(*leafNodeS)
			if !validLeafNode(node, t.order) {
				lgr.Printf("!validLeafNode(node, t.order) node=\n%v", node)
				return false
			}
		} else {
			node := nodes[i].(*interiorNodeS)
			if !validInteriorNode(node, t.order) {
				lgr.Printf("!validInteriorNode(node, t.order) node=\n%v", node)
				return false
			}
			nodes = append(nodes, node.vals...)
		}
	}
	return true
}

func validRootNode(node nodeI, order int) bool {
	if node.isLeaf() {
		node := node.(*leafNodeS)

		if !(len(node.keys) >= 0 && len(node.keys) <= order-1) {
			lgr.Printf("!(len(node.keys),%d >= 0 && len(node.keys),%d <= order-1,%d) root=\n%v", len(node.keys), len(node.keys), cap(node.keys)-1, node)
			return false
		}
		if len(node.keys) != len(node.vals) {
			lgr.Printf("len(node.keys),%d != len(node.vals),%d root=\n%v", len(node.keys), len(node.vals), node)
			return false
		}
	} else {
		node := node.(*interiorNodeS)

		if !(len(node.keys) >= 1 && len(node.keys) <= order-1) {
			lgr.Printf("validRootNode: !(len(node.keys),%d >= 1 && len(node.keys),%d <= order-1,%d) root=\n%v", len(node.keys), len(node.keys), cap(node.keys)-1, node)
			return false
		}
		if len(node.keys) != len(node.vals)-1 {
			lgr.Printf("len(node.keys),%d != len(node.vals)-1,%d root=\n%v", len(node.keys), len(node.vals)-1, node)
			return false
		}
	}
	return true
}

func validInteriorNode(n_ nodeI, order int) bool {
	node, ok := n_.(*interiorNodeS)
	if !ok {
		lgr.Printf("The Node passed in is not castable to *interiorNodeS")
		return false
	}

	if !validNodeKeys(node.keys, order) {
		lgr.Printf("!validNodeKeys(t, node.keys, order) node=\n%v", node)
		return false
	}
	if !validNodeVals(node.vals, order) {
		lgr.Printf("!validNodeVals(t, node.vals, order) node=\n%v", node)
		return false
	}
	if len(node.keys) != len(node.vals)-1 {
		lgr.Printf("len(node.keys),%d != len(node.vals)-1,%d node=\n%v", len(node.keys), len(node.vals)-1, node)
		return false
	}
	return true
}

func validLeafNode(node_ nodeI, order int) bool {
	node, ok := node_.(*leafNodeS)
	if !ok {
		lgr.Printf("The Node passed in is not castable to *leafNodeS")
		return false
	}

	if !validLeafKeys(node.keys, order) {
		lgr.Printf("!validLeafKeys(node.keys, order) node=\n%v", node)
		return false
	}
	if !validLeafVals(node.vals, order) {
		lgr.Printf("!validLeafVals(node.vals, order) node=\n%v", node)
		return false
	}
	if len(node.keys) != len(node.vals) {
		lgr.Printf("len(node.keys),%d != len(node.vals),%d node=\n%v", len(node.keys), len(node.vals), node)
		return false
	}
	return true
}

func intCeil(n, d int) int {
	return int(math.Ceil(float64(n) / float64(d)))
}

func validLeafKeys(keys []BptKey, order int) bool {
	if !(len(keys) >= intCeil(order-1, 2) && len(keys) <= order-1) {
		lgr.Printf("!(len(keys),%d >= intCeil(order-1, 2),%d && len(keys),%d <= order-1),%d", len(keys), intCeil(order-1, 2), len(keys), order)
		return false
	}
	return true
}

func validLeafVals(vals []interface{}, order int) bool {
	if !(len(vals) >= intCeil(order-1, 2) && len(vals) <= order-1) {
		lgr.Printf("!(len(vals),%d >= intCeil(order-1, 2),%d && len(vals),%d <= order-1),%d", len(vals), intCeil(order-1, 2), len(vals), order)
		return false
	}
	return true
}

func validNodeKeys(keys []BptKey, order int) bool {
	if !(len(keys) >= intCeil(order, 2)-1 && len(keys) <= order-1) {
		lgr.Printf("!(len(keys),%d >= intCeil(order, 2)-1,%d && len(keys),%d <= order-1),%d", len(keys), intCeil(order, 2)-1, len(keys), order-1)
		return false
	}
	return true
}

func validNodeVals(vals []nodeI, order int) bool {
	if !(len(vals) >= intCeil(order, 2) && len(vals) <= order) {
		lgr.Printf("!(len(vals),%d >= intCeil(order, 2),%d && len(vals),%d <= order,%d)", len(vals), intCeil(order, 2), len(vals), order)
		return false
	}
	return true
}
