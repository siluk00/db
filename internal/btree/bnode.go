package btree

import (
	"bytes"
	"encoding/binary"
)

// header consists of bNode + number of Keys
const HEADER = 4

type BNode []byte

// bType types
const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

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
	root     uint64              //Pointer to root
	get      func(uint64) []byte //get page from pointer
	newBNode func([]byte) uint64 //allocate a pointer to page
	del      func(uint64)        //deallocate a page
}

// Btypes Node or leaf
func (node BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// Number of keys in KV, in case of BNODE_NODE nKeys = number of childs
func (node BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// Header of 4 bytes = btype(2 bytes) : nKeys(2 bytes)
func (node BNode) setHeader(btype, nKeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nKeys)
}

// After header theres a list of pointers to the child nodes in the case B_NODE_NODE btype
func (node BNode) getPtr(idx uint16) uint64 {
	if !(idx < node.nKeys()) {
		panic("nKeys should be greater then index")
	}

	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

// Sets the pointer to child
func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// helper function for getOffset in case index != 0
func offsetPos(node BNode, idx uint16) uint16 {
	if !(1 <= idx && idx <= node.nKeys()) {
		panic("")
	}

	return HEADER + 8*node.nKeys() + 2*(idx-1)
}

// if the index is zero, the offset position of the kv pair is just 0;
// offset implements the position of the kv pair inside the BNode struct
// so the search is O(1)
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

// Sets the offset of idx to offset
func (node BNode) setOffset(idx, offset uint16) {
	if idx == 0 || idx > node.nKeys() {
		panic("")
	}

	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

// Returns the position in wich the kv is stored on BNode based on idx
func (node BNode) kvPos(idx uint16) uint16 {
	if !(idx <= node.nKeys()) {
		panic("")
	}

	return HEADER + 8*node.nKeys() + 2*node.nKeys() + node.getOffset(idx)
}

// gets key by idx by searching first the kvposition
// after HEADER + ptr + offset theres the key with 2 bytes for klen and 2 bytes for vlen
// then slices with value of klen
func (node BNode) getKey(idx uint16) []byte {
	if !(idx < node.nKeys()) {
		panic("")
	}

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

// gets the value after Key
func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// returns the last index written on BNode
func (node BNode) nBytes() uint16 {
	return node.kvPos(node.nKeys())
}

// TODO: Binary Search
// Searches for key child inside BNode, returns the first child node whose range intersects key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nKeys := node.nKeys()
	found := uint16(0)

	for i := uint16(1); i < nKeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}

		if cmp >= 0 {
			break
		}
	}

	return found
}

// adds new kv inside a leaf
func leafInsert(newBNode, oldBNode BNode, idx uint16, key, val []byte) {
	newBNode.setHeader(BNODE_LEAF, oldBNode.nKeys()+1)
	nodeAppendRange(newBNode, oldBNode, 0, 0, idx)
	nodeAppendKV(newBNode, idx, 0, key, val)
	nodeAppendRange(newBNode, oldBNode, idx+1, idx, oldBNode.nKeys()-idx)
}

// sets child pointer for BNODE_NODE
// sets key/value to position pointed by offset and sets offset of the next kv for BNODE_LEAF
func nodeAppendKV(newBNode BNode, idx uint16, ptr uint64, key, val []byte) {
	newBNode.setPtr(idx, uint64(ptr))
	pos := newBNode.kvPos(idx)
	binary.LittleEndian.PutUint16(newBNode[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(newBNode[pos+2:], uint16(len(val)))
	copy(newBNode[pos+4:], key)
	copy(newBNode[pos+4+uint16(len(key)):], val)

	newBNode.setOffset(idx+1, uint16(newBNode.getOffset(idx)+4+uint16((len(key)+len(val)))))
}

// appends a range of n KV from oldBnode to newBnode strarting from srcOld
func nodeAppendRange(newBNode, oldBNode BNode, dstNew, srcOld, n uint16) {
	if n == 0 {
		return
	}

	// Copy offsets, pointers, and KV data for the range
	for i := uint16(0); i < n; i++ {
		srcIdx := srcOld + i
		dstIdx := dstNew + i

		// Copy pointer (for internal nodes) or set to 0 (for leaf nodes)
		if oldBNode.bType() == BNODE_NODE {
			newBNode.setPtr(dstIdx, oldBNode.getPtr(srcIdx))
		} else {
			newBNode.setPtr(dstIdx, 0)
		}

		// Copy key-value data
		key := oldBNode.getKey(srcIdx)
		val := oldBNode.getVal(srcIdx)
		pos := newBNode.kvPos(dstIdx)
		binary.LittleEndian.PutUint16(newBNode[pos:], uint16(len(key)))
		binary.LittleEndian.PutUint16(newBNode[pos+2:], uint16(len(val)))
		copy(newBNode[pos+4:], key)
		copy(newBNode[pos+4+uint16(len(key)):], val)

		// Update offset for next position
		newBNode.setOffset(dstIdx+1, newBNode.getOffset(dstIdx)+4+uint16(len(key)+len(val)))
	}
}

func nodeAppendKidN(tree *BTree, newBNode, oldBNode BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	newBNode.setHeader(BNODE_NODE, oldBNode.nKeys()+inc-1)
	nodeAppendRange(newBNode, oldBNode, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(newBNode, idx+uint16(i), tree.newBNode(node), node.getKey(0), nil)
	}
	nodeAppendRange(newBNode, oldBNode, idx+inc, idx+1, oldBNode.nKeys()-(idx+1))
}

// review
func nodeSplit2(left BNode, right BNode, old BNode) {
	nKeys := old.nKeys()

	// Find the optimal split point
	splitIdx := uint16(0)
	totalSize := old.nBytes()
	targetSize := totalSize / 2

	// Calculate cumulative sizes to find the best split point
	currentSize := uint16(0)
	for i := uint16(0); i < nKeys; i++ {
		keySize := uint16(len(old.getKey(i)))
		valSize := uint16(len(old.getVal(i)))
		kvSize := 4 + keySize + valSize // 4 bytes for keyLen + valLen

		if currentSize+kvSize > targetSize && i > 0 {
			splitIdx = i
			break
		}
		currentSize += kvSize
	}

	// If we didn't find a good split point, split in the middle
	if splitIdx == 0 {
		splitIdx = nKeys / 2
	}

	// Ensure we have at least one key in each split
	if splitIdx == 0 {
		splitIdx = 1
	}
	if splitIdx == nKeys {
		splitIdx = nKeys - 1
	}

	// Copy data to left and right nodes
	left.setHeader(old.bType(), splitIdx)
	right.setHeader(old.bType(), nKeys-splitIdx)

	// Copy the appropriate ranges
	if old.bType() == BNODE_LEAF {
		nodeAppendRange(left, old, 0, 0, splitIdx)
		nodeAppendRange(right, old, 0, splitIdx, nKeys-splitIdx)
	} else {
		// For internal nodes, copy child pointers
		for i := uint16(0); i < splitIdx; i++ {
			left.setPtr(i, old.getPtr(i))
			key := old.getKey(i)
			val := old.getVal(i)
			nodeAppendKV(left, i, old.getPtr(i), key, val)
		}
		for i := splitIdx; i < nKeys; i++ {
			right.setPtr(i-splitIdx, old.getPtr(i))
			key := old.getKey(i)
			val := old.getVal(i)
			nodeAppendKV(right, i-splitIdx, old.getPtr(i), key, val)
		}
	}
}

// Splits the old Bnode into 1, 2, or 3 Bnodes, and returns the splitten nodes together with the number of nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nBytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)

	if left.nBytes() <= BTREE_PAGE_SIZE {
		left := left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	if !(leftleft.nBytes() <= BTREE_PAGE_SIZE) {
		panic("")
	}

	return 3, [3]BNode{left, middle, right}
}

// Inserts
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

// TODO: change leaf update
// leafUpdate copies everything from node to newBnode, but, it updates the value on key passed
func leafUpdate(newBNode, node BNode, idx uint16, key, val []byte) {
	newBNode.setHeader(BNODE_LEAF, node.nKeys())
	nodeAppendRange(newBNode, node, 0, 0, idx)
	nodeAppendKV(newBNode, idx, 0, key, val)
	nodeAppendRange(newBNode, node, idx+1, idx+1, node.nKeys()-(idx+1))
}

func nodeInsert(tree *BTree, newBNode, node BNode, idx uint16, key, val []byte) {
	kptr := node.getPtr(idx)
	knode := treeInsert(tree, tree.get(kptr), key, val)
	nsplit, split := nodeSplit3(knode)
	tree.del(kptr)
	nodeReplaceKidN(tree, newBNode, node, idx, split[:nsplit]...)
}

func nodeReplaceKidN(tree *BTree, newBNode, oldBNode BNode, idx uint16, kids ...BNode) {
	newBNode.setHeader(BNODE_NODE, oldBNode.nKeys()+uint16(len(kids))-1)

	// Copy nodes before the replacement point
	nodeAppendRange(newBNode, oldBNode, 0, 0, idx)

	// Insert the new kids
	for i, kid := range kids {
		nodeAppendKV(newBNode, idx+uint16(i), tree.newBNode(kid), kid.getKey(0), nil)
	}

	// Copy nodes after the replacement point
	nodeAppendRange(newBNode, oldBNode, idx+uint16(len(kids)), idx+1, oldBNode.nKeys()-(idx+1))
}
