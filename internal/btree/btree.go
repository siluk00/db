package btree

import "encoding/binary"

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if !(node1max <= BTREE_PAGE_SIZE) {
		panic("node1max larger than page size")
	}
}

type BNode []byte

type BTree struct {
	root     uint64
	get      func(uint64) []byte
	newBTree func([]byte) uint64
	del      func(uint64)
}

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
