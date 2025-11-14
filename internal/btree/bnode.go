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

func (node BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype, nKeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nKeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	if !(idx < node.nKeys()) {
		panic("nKeys should be greater then index")
	}

	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

// HEY
func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

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

func (node BNode) setOffset(idx, offset uint16) {

}

func (node BNode) kvPos(idx uint16) uint16 {
	if !(idx <= node.nKeys()) {
		panic("")
	}

	return HEADER + 8*node.nKeys() + 2*node.nKeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if !(idx < node.nKeys()) {
		panic("")
	}

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

// TODO
func (node BNode) getVal(idx uint16) []byte {
	return []byte{0}
}

func (node BNode) nBytes() uint16 {
	return node.kvPos(node.nKeys())
}

// TODO: Binary Search
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

func leafInsert(newBNode, oldBNode BNode, idx uint16, key, val []byte) {
	newBNode.setHeader(BNODE_LEAF, oldBNode.nKeys()+1)
	nodeAppendRange(newBNode, oldBNode, 0, 0, idx)
	nodeAppendKV(newBNode, idx, 0, key, val)
	nodeAppendRange(newBNode, oldBNode, idx+1, idx, oldBNode.nKeys()-idx)

}

func nodeAppendKV(newBNode BNode, idx uint16, ptr uint64, key, val []byte) {
	newBNode.setPtr(idx, uint64(ptr))

	pos := newBNode.kvPos(idx)
	binary.LittleEndian.PutUint16(newBNode[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(newBNode[pos+2:], uint16(len(val)))
	copy(newBNode[pos+4:], key)
	copy(newBNode[pos+4+uint16(len(key)):], val)

	newBNode.setOffset(idx+1, uint16(newBNode.getOffset(idx)+4+uint16((len(key)+len(val)))))
}

func nodeAppendRange(newBNode, oldBNode BNode, dstNew, srcOld, n uint16) {

}

func nodeAppendKidN(tree *BTree, newBNode, oldBNode BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	newBNode.setHeader(BNODE_NODE, oldBNode.nKeys()+inc-1)
	nodeAppendRange(newBNode, oldBNode, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(newBNode, idx+uint16(i), tree.newBTree(node), node.getKey(0), nil)
	}
	nodeAppendRange(newBNode, oldBNode, idx+inc, idx+1, oldBNode.nKeys()-(idx+1))
}

func nodeSplit2(left BNode, right BNode, old BNode) {

}

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
