package btree

import (
	"bytes"
)

// Inserts key, val into BTree
func (tree *BTree) Insert(key, val []byte) {
	// If BTree is empty insert a dummy key plus the key, val passed as parameters
	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)

		//dummy key, so the tree covers the whole key space
		//thus lookup can always find a containing node
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		var err error
		tree.root, err = tree.newBNode(root)
		if err != nil {
			panic("")
		}
		return
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)

		for i, knode := range split[:nsplit] {
			ptr, err := tree.newBNode(knode)
			if err != nil {
				panic("")
			}
			key := knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}

		var err error
		tree.root, err = tree.newBNode(root)
		if err != nil {
			panic("")
		}
	} else {
		var err error
		tree.root, err = tree.newBNode(split[0])
		if err != nil {
			panic("")
		}
	}
}

func (tree *BTree) Delete(key []byte) bool {
	if tree.root == 0 {
		return false
	}

	updated := treeDelete(tree, BNode(tree.get(tree.root)), key)
	if len(updated) == 0 {
		// Key not found
		return false
	}

	tree.del(tree.root)

	if updated.nKeys() == 0 {
		// Tree became empty
		tree.root = 0
	} else {
		var err error
		tree.root, err = tree.newBNode(updated)
		if err != nil {
			panic("")
		}
	}

	return true
}

// Gets the val for the key, returns true if key is found
func (tree *BTree) Get(key []byte) ([]byte, bool) {
	root := BNode(tree.get(tree.root))
	idx := nodeLookupLE(root, key)
	//fmt.Println(string(key), idx)

	if root.bType() == BNODE_LEAF {
		idx, ok := nodeLookupE(root, key)
		if ok {
			return root.getVal(idx), true
		}
		return nil, false
	}

	offset := BNode(root).getOffset(idx)
	node := BNode(tree.get(uint64(offset)))

	for node.bType() == BNODE_NODE {
		idx = nodeLookupLE(node, key)
		offset = BNode(node).getOffset(idx)
		node = BNode(tree.get(uint64(offset)))
	}

	idx = nodeLookupLE(node, key)
	if idx == 0 {
		return nil, false
	}

	return node.getVal(idx), true
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nKeys()-1)

	// Copy all keys before the deleted one
	nodeAppendRange(new, old, 0, 0, idx)
	// Copy all keys after the deleted one
	nodeAppendRange(new, old, idx, idx+1, old.nKeys()-(idx+1))
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.bType(), left.nKeys()+right.nKeys())

	// Copy all keys from left node
	nodeAppendRange(new, left, 0, 0, left.nKeys())
	// Copy all keys from right node
	nodeAppendRange(new, right, left.nKeys(), 0, right.nKeys())
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(
	new BNode, old BNode, idx uint16, ptr uint64, key []byte,
) {
	new.setHeader(BNODE_NODE, old.nKeys()-1)

	// Copy nodes before the replacement point
	nodeAppendRange(new, old, 0, 0, idx)
	// Insert the merged node
	nodeAppendKV(new, idx, ptr, key, nil)
	// Copy nodes after the replacement point (skip one)
	nodeAppendRange(new, old, idx+1, idx+2, old.nKeys()-(idx+2))
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nBytes() + updated.nBytes() - HEADER

		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}

	if idx+1 < node.nKeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nBytes() + updated.nBytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}

	return 0, BNode{}
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)

	switch node.bType() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			// Key found in leaf - delete it
			new := BNode(make([]byte, BTREE_PAGE_SIZE))
			leafDelete(new, node, idx)
			return new
		} else {
			// Key not found
			return BNode{}
		}

	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)

	default:
		panic("bad node")
	}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)

	if len(updated) == 0 {
		return BNode{}
	}
	tree.del(kptr)

	newBnode := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		mergedBNode, err := tree.newBNode(merged)
		if err != nil {
			panic("")
		}
		nodeReplace2Kid(newBnode, node, idx-1, mergedBNode, merged.getKey(0))
	case mergeDir > 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		mergedBNode, err := tree.newBNode(merged)
		if err != nil {
			panic("")
		}
		nodeReplace2Kid(newBnode, node, idx, mergedBNode, merged.getKey(0))
	case mergeDir == 0 && updated.nKeys() == 0:
		if !(node.nKeys() == 1 && idx == 0) {
			panic("")
		}
		newBnode.setHeader(BNODE_NODE, 0)
	case mergeDir == 0 && updated.nKeys() > 0:
		nodeReplaceKidN(tree, newBnode, node, idx, updated)
	}

	return newBnode
}
