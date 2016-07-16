package bptree

import (
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/lleo/util"
)

type entry struct {
	key BptKey
	val int
}

var genRandomizedEntries func(ents []entry) []entry

var midNumEnts []entry
var largeNumEnts []entry

func TestMain(m *testing.M) {
	//SETUP
	genRandomizedEntries = genRandomizedEntriesInPlace

	midNumEnts = make([]entry, 0, 32) //binary growth
	s := util.Str("")
	nEnts := 10000 //ten thousand
	//nEnts := 1000
	for i := 0; i < nEnts; i++ {
		s = s.Inc(1) //get off "" first
		midNumEnts = append(midNumEnts, entry{StringKey(string(s)), i + 1})
	}

	largeNumEnts = make([]entry, 0, 32) //binary growth
	s = util.Str("")
	//nEnts = 1000000 //one million
	nEnts = 100000 //one hundred thousand
	for i := 0; i < nEnts; i++ {
		s = s.Inc(1) //get off "" first
		largeNumEnts = append(largeNumEnts, entry{StringKey(string(s)), i + 1})
	}

	util.RandSeed() //seeds rand.Seed() with time.Now().UnixNano()

	xit := m.Run()

	//TEARDOWN

	os.Exit(xit)
}

func TestValidInOrderPutTree(t *testing.T) {
	bpt := NewBpTree(3)
	var added bool
	for _, ent := range largeNumEnts {
		bpt, added = bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("TestValidInOrderTree: bpt should be a fresh tree and every Put() should be added==true")
			t.Fail()
		}
	}
	bptv := bpt.(*tree)
	if !_validTree(t, bptv) {
		t.Logf("TREE =\n%v", bptv)
		t.Fail()
	}
}

func TestValidRandomPutTree(t *testing.T) {
	bpt := NewBpTree(3)
	var added bool
	randEnts := genRandomizedEntries(largeNumEnts)
	for _, ent := range randEnts {
		bpt, added = bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("TestValidInOrderTree: bpt should be a fresh tree and every Put() should be added==true")
			t.Fail()
		}
	}
	bptv := bpt.(*tree)
	if !_validTree(t, bptv) {
		t.Logf("TREE =\n%v", bptv)
		t.Fail()
	}
}

func TestInOrderPutOWithInOrderGet(t *testing.T) {
	bpt := NewBpTree(3)
	//bpt := NewBpTree(7)
	var added bool
	for _, ent := range largeNumEnts {
		bpt, added = bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("TestValidInOrderTree: bpt should be a fresh tree and every Put() should be added==true")
			t.Fail()
		}
	}
	for _, ent := range largeNumEnts {
		v, found := bpt.Get(ent.key)
		if !found {
			t.Logf("TestValidInOrderTree: bpt.Get() ent.key=%q was not found", ent.key)
			t.Fail()
		} else if v != ent.val {
			t.Logf("TestValidInOrderTree: bpt.Get(ent.key=%q) found a incorrect value; ent.val=%d; v=%d", ent.key, ent.val, v)
			t.Fail()
		}
	}
}

func TestRandomPutWithRandomGet(t *testing.T) {
	bpt := NewBpTree(3)
	//bpt := NewBpTree(7)
	var added bool
	randEntsPut := genRandomizedEntries(largeNumEnts)
	for _, ent := range randEntsPut {
		bpt, added = bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("TestValidInOrderTree: bpt should be a fresh tree and every Put() should be added==true")
			t.Fail()
		}
	}
	randEntsGet := genRandomizedEntries(largeNumEnts)
	for _, ent := range randEntsGet {
		val, found := bpt.Get(ent.key)
		if !found {
			t.Logf("TestValidInOrderTree: bpt.Get() ent.key=%q was not found", ent.key)
			t.Fail()
		} else if val != ent.val {
			t.Logf("TestValidInOrderTree: bpt.Get(ent.key=%q) found a incorrect value; ent.val=%d; val=%d", ent.key, ent.val, val)
			t.Fail()
		}
	}
}

func TestInOrderPutWithInOrderDel(t *testing.T) {
	bpt := NewBpTree(3)
	//bpt := NewBpTree(7)
	var added bool
	for _, ent := range largeNumEnts {
		bpt, added = bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("TestInOrderPutWithInOrderDel: bpt should be a fresh tree and every Put() should be added=true")
			t.Fail()
		}
	}
	var val interface{}
	var found bool
	for _, ent := range largeNumEnts {
		bpt, val, found = bpt.Del(ent.key)
		if !found {
			t.Logf("TestInOrderPutWithInOrderDel: bpt.Del() ent.key=%q was not found", ent.key)
			t.Fail()
		} else if val != ent.val {
			t.Logf("TestInOrderPutWithInOrderDel: bpt.Del(ent.key=%q) found a incorrect value; ent.val=%d; val=%d", ent.key, ent.val, val)
			t.Fail()
		}
	}
	if !bpt.IsEmpty() {
		t.Logf("bpt.IsEmpty() returned false")
		t.Fail()
	}
}

func TestRandomPutWithRandomDel(t *testing.T) {
	bpt := NewBpTree(3)
	//bpt := NewBpTree(7)
	var added bool
	randEntsPut := genRandomizedEntries(largeNumEnts)
	for _, ent := range randEntsPut {
		bpt, added = bpt.Put(ent.key, ent.val)
		if !added {
			t.Logf("TestRandomPutWithRandomDel: bpt should be a fresh tree and every Put() should be added=true")
			t.Fail()
		}
	}
	var val interface{}
	var found bool
	randEntsDel := genRandomizedEntries(largeNumEnts)
	for _, ent := range randEntsDel {
		bpt, val, found = bpt.Del(ent.key)
		if !found {
			t.Logf("TestRandomPutWithRandomDel: bpt.Del() ent.key=%q was not found", ent.key)
			t.Fail()
		} else if val != ent.val {
			t.Logf("TestRandomPutWithRandomDel: bpt.Del(ent.key=%q) found a incorrect value; ent.val=%d; val=%d", ent.key, ent.val, val)
			t.Fail()
		}
	}
	if !bpt.IsEmpty() {
		t.Logf("TestRandomPutWithRandomDel: bpt.IsEmpty() returned false")
		t.Fail()
	}
}

func TestRandomPutAndDelWithOrderThreeToSixtyFour(t *testing.T) {
	for order := 3; order < 65; order++ {
		bpt := NewBpTree(order)
		var added bool
		randEntsPut := genRandomizedEntries(midNumEnts)
		//randEntsPut := genRandomizedEntries(midNumEnts)
		for _, ent := range randEntsPut {
			bpt, added = bpt.Put(ent.key, ent.val)
			if !added {
				t.Logf("bpt should be a fresh tree and every Put() should be added=true")
				t.Fail()
			}
		}
		var val interface{}
		var found bool
		randEntsDel := genRandomizedEntries(midNumEnts)
		//randEntsDel := genRandomizedEntries(largeNumEnts)
		for _, ent := range randEntsDel {
			bpt, val, found = bpt.Del(ent.key)
			if !found {
				t.Logf("bpt.Del() ent.key=%q was not found", ent.key)
				t.Fail()
			} else if val != ent.val {
				t.Logf("bpt.Del(ent.key=%q) found a incorrect value; ent.val=%d; val=%d", ent.key, ent.val, val)
				t.Fail()
			}
		}
		if !bpt.IsEmpty() {
			t.Logf("bpt.IsEmpty() returned false")
			t.Fail()
		}

	}
}

//func TestRandomPutWithRandomCursor(t *testing.T) {
//	bpt := NewBpTree(7)
//	var added bool
//	randEntsPut := genRandomizedEntries(largeNumEnts)
//	for _, ent := range randEntsPut {
//		bpt, added = bpt.Put(ent.key, ent.val)
//		if !added {
//			t.Logf("TestValidInOrderTree: bpt should be a fresh tree and every Put() should be added==true")
//			t.Fail()
//		}
//	}
//	randEntsGet := genRandomizedEntries(largeNumEnts)
//	for _, ent := range randEntsGet {
//		val, found := bpt.Get(ent.key)
//		if !found {
//			t.Logf("TestValidInOrderTree: bpt.Get() ent.key=%q was not found", ent.key)
//			t.Fail()
//		} else if val != ent.val {
//			t.Logf("TestValidInOrderTree: bpt.Get(ent.key=%q) found a incorrect value; ent.val=%d; val=%d", ent.key, ent.val, val)
//			t.Fail()
//		}
//	}
//
//}

func genRandomizedEntriesTmpSlice(ents []entry) []entry {
	tmpEnts := make([]entry, len(ents))
	copy(tmpEnts, ents)

	randomEnts := make([]entry, 0, len(ents))
	for len(tmpEnts) > 0 {
		i := rand.Intn(len(tmpEnts))
		randomEnts = append(randomEnts, tmpEnts[i])
		//cut out i'th element from tmpEnts
		tmpEnts = append(tmpEnts[:i], tmpEnts[i+1:]...)
		//tmpEnts = tmpEnts[:i+copy(tmpEnts[i:], tmpEnts[i+1:])]
	}
	return randomEnts
}

//First genRandomizedEntries() copies []entry passed in. Then it randomizes that
//copy in-place. Finnally, it returns the randomized copy.
func genRandomizedEntriesInPlace(ents []entry) []entry {
	randEnts := make([]entry, len(ents))
	copy(randEnts, ents)

	//From: https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle#The_modern_algorithm
	for i := len(randEnts) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		randEnts[i], randEnts[j] = randEnts[j], randEnts[i]
	}

	return randEnts
}

func _validTree(test *testing.T, t_ BpTree) bool {
	t, ok := t_.(*tree)
	if !ok {
		test.Log("failed to cast BpTree to *tree")
		test.Fail()
	}

	if !_validRootNode(test, t.root, t.order) {
		return false
	}
	if t.root.isLeaf() {
		return true //else _validRootNode(t.root) would have caught it
	}

	rootNode := t.root.(*interiorNodeS)

	nodes := make([]nodeI, 0, 2)

	//seed the nodes slice
	for i := 0; i < len(rootNode.vals); i++ {
		nodes = append(nodes, rootNode.vals[i])
	}

	for i := 0; i < len(nodes); i++ {
		if nodes[i].isLeaf() {
			n := nodes[i].(*leafNodeS)
			if !_validLeafNode(test, n, t.order) {
				test.Logf("!_validLeafNode(test, n, t.order) n=\n%v", n)
				return false
			}
		} else {
			n := nodes[i].(*interiorNodeS)
			if !_validInteriorNode(test, n, t.order) {
				test.Logf("!_validInteriorNode(test, n, t.order) n=\n%v", n)
				return false
			}
			nodes = append(nodes, n.vals...)
		}
	}
	return true
}

func _validRootNode(t *testing.T, n nodeI, order int) bool {
	if n.isLeaf() {
		n := n.(*leafNodeS)
		if !(len(n.keys) >= 0 && len(n.keys) <= order-1) {
			t.Logf("!(len(n.keys) >= 1 && len(n.keys) <= order-1) n=\n%v", n)
			return false
		}
		if len(n.keys) != len(n.vals) {
			t.Logf("len(n.keys) != len(n.vals) n=\n%v", n)
			return false
		}
	} else {
		n := n.(*interiorNodeS)
		if !(len(n.keys) >= 1 && len(n.keys) <= order-1) {
			t.Logf("!(len(n.keys),%d >= 1 && len(n.keys),%d <= order-1,%d)", len(n.keys), len(n.keys), order-1)
			return false
		}
		if len(n.keys) != len(n.vals)-1 {
			t.Logf("len(n.keys),%d != len(n.vals)-1,%d", len(n.keys), len(n.vals)-1)
			return false
		}
	}
	return true
}

func _validInteriorNode(t *testing.T, node_ nodeI, order int) bool {
	node, ok := node_.(*interiorNodeS)
	if !ok {
		lgr.Printf("The nodeI passed in is not castable to *interiorNodeS")
		return false
	}

	if !_validNodeKeys(t, node.keys, order) {
		t.Logf("!_validNodeKeys(t, node.keys, order) node=\n%v", node)
		return false
	}
	if !_validNodeVals(t, node.vals, order) {
		t.Logf("!_validNodeVals(t, node.vals, order) node=\n%v", node)
		return false
	}

	if len(node.keys) != len(node.vals)-1 {
		t.Logf("len(node.keys),%d != len(node.vals)-1,%d node=\n%v", len(node.keys), len(node.vals)-1, node)
		return false
	}
	return true
}

func _validLeafNode(t *testing.T, node_ nodeI, order int) bool {
	node, ok := node_.(*leafNodeS)
	if !ok {
		lgr.Printf("The nodeI passed in is not castable to *leafNodeS")
		return false
	}

	if !_validLeafKeys(t, node.keys, order) {
		t.Logf("!_validLeafKeys(t, node.keys, order) node=\n%v", node)
		return false
	}
	if !_validLeafVals(t, node.vals, order) {
		t.Logf("!_validLeafVals(t, node.vals, order) node=\n%v", node)
		return false
	}
	if len(node.keys) != len(node.vals) {
		t.Logf("len(node.keys),%d != len(node.vals),%d node=\n%v", len(node.keys), len(node.vals), node)
		return false
	}
	return true
}

func _validLeafKeys(t *testing.T, keys []BptKey, order int) bool {
	if !(len(keys) >= _intCeil(order-1, 2) && len(keys) <= order-1) {
		t.Logf("!(len(keys),%d >= _intCeil(order-1, 2),%d && len(keys),%d <= order-1),%d", len(keys), _intCeil(order-1, 2), len(keys), order)
		return false
	}
	return true
}

func _validLeafVals(t *testing.T, vals []interface{}, order int) bool {
	if !(len(vals) >= _intCeil(order-1, 2) && len(vals) <= order-1) {
		t.Logf("!(len(vals),%d >= _intCeil(order-1, 2),%d && len(vals),%d <= order-1),%d", len(vals), _intCeil(order-1, 2), len(vals), order)
		return false
	}
	return true
}

func _validNodeKeys(t *testing.T, keys []BptKey, order int) bool {
	if !(len(keys) >= _intCeil(order, 2)-1 && len(keys) <= order-1) {
		t.Logf("!(len(keys),%d >= _intCeil(order, 2)-1,%d && len(keys),%d <= order-1),%d", len(keys), _intCeil(order, 2)-1, len(keys), order)
		return false
	}
	return true
}

func _validNodeVals(t *testing.T, vals []nodeI, order int) bool {
	if !(len(vals) >= _intCeil(order, 2) && len(vals) <= order) {
		t.Logf("!(len(vals),%d >= _intCeil(order, 2),%d && len(vals),%d <= order,%d)", len(vals), _intCeil(order, 2), len(vals), order)
		return false
	}
	return true
}

func _intCeil(n, d int) int {
	return int(math.Ceil(float64(n) / float64(d)))
}
