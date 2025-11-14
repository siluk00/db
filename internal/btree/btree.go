package btree

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
