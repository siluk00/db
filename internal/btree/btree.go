package btree

import "bytes"

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if !(node1max <= BTREE_PAGE_SIZE) {
		panic("node1max larger than page size")
	}
}

type BTree struct {
	root     uint64
	get      func(uint64) []byte
	newBTree func([]byte) uint64
	del      func(uint64)
}

func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	newBNode := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	idx := nodeLookupLE(node, key)

	switch node.bType() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(newBNode, node, idx, key, val)
		} else {
			leafInsert(newBNode, node, idx+1, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, newBNode, node, idx, key, val)
	default:
		panic("bad node")
	}

	return newBNode
}

func leafUpdate(newBNode, node BNode, idx uint16, key, val []byte) {}

func nodeInsert(tree *BTree, newBNode, node BNode, idx uint16, key, val []byte) {
	kptr := node.getPtr(idx)
	knode := treeInsert(tree, tree.get(kptr), key, val)
	nsplit, split := nodeSplit3(knode)
	tree.del(kptr)
	nodeReplaceKidN(tree, newBNode, node, idx, split[:nsplit]...)
}

func nodeReplaceKidN(tree *BTree, newBNode, oldBNode BNode, idx uint16, kids ...BNode) {}
