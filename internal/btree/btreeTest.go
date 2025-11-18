package btree

import (
	"testing"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func testTree(t *testing.T) {

}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				if !ok {
					panic("")
				}
				return node
			},
			newBNode: func(node []byte) uint64 {
				if !(BNode(node).nBytes() <= BTREE_PAGE_SIZE) {
					panic("")
				}
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				if !(pages[ptr] == nil) {
					panic("")
				}
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				if !(pages[ptr] != nil) {
					panic("")
				}
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}
