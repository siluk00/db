package btree

func (tree *BTree) Insert(key, val []byte) {
	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)

		//dummy key, so the tree covers the whole key space
		//thus lookup can always find a containing node
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.newBNode(root)
		return
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)

		for i, knode := range split[:nsplit] {
			ptr, key := tree.newBNode(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}

		tree.root = tree.newBNode(root)
	} else {
		tree.root = tree.newBNode(split[0])
	}
}

func (tree *BTree) Delete(key []byte) bool {
	return false
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16)

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode)

// replace 2 adjacent links with 1
func nodeReplace2Kid(
	new BNode, old BNode, idx uint16, ptr uint64, key []byte,
)

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
	return BNode{}
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
		nodeReplace2Kid(newBnode, node, idx-1, tree.newBNode(merged), merged.getKey(0))
	case mergeDir > 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(newBnode, node, idx, tree.newBNode(merged), merged.getKey(0))
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
