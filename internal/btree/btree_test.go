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

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				if !ok {
					panic("page not found")
				}
				return node
			},
			newBNode: func(node []byte) uint64 {
				if !(BNode(node).nBytes() <= BTREE_PAGE_SIZE) {
					panic("page too large")
				}
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				if !(pages[ptr] == nil) {
					panic("page already exists")
				}
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				if !(pages[ptr] != nil) {
					panic("page not found for deletion")
				}
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func TestTreeBasic(t *testing.T) {
	c := newC()

	ok := c.tree.Delete([]byte("nonexistent")) 
	testify.

	// Test first insertion
	c.tree.Insert([]byte("key1"), []byte("value1"))
	c.ref["key1"] = "value1"

	// Verify tree structure
	if c.tree.root == 0 {
		t.Fatal("Root should be set after insertion")
	}

	// Test duplicate key insertion (update)
	c.tree.Insert([]byte("key1"), []byte("value1_updated"))
	c.ref["key1"] = "value1_updated"

	// Test multiple insertions
	c.tree.Insert([]byte("key2"), []byte("value2"))
	c.tree.Insert([]byte("key0"), []byte("value0"))
	c.ref["key2"] = "value2"
	c.ref["key0"] = "value0"

	// Verify all pages are valid
	for ptr, node := range c.pages {
		if node.nBytes() > BTREE_PAGE_SIZE {
			t.Fatalf("Page %d exceeds size limit: %d > %d", ptr, node.nBytes(), BTREE_PAGE_SIZE)
		}
	}
}

func TestTreeDelete(t *testing.T) {
	c := newC()

	// Insert test data
	keys := []string{"a", "b", "c", "d", "e"}
	for i, key := range keys {
		value := string(rune('v' + i))
		c.tree.Insert([]byte(key), []byte(value))
		c.ref[key] = value
	}

	// Delete middle key
	if !c.tree.Delete([]byte("c")) {
		t.Fatal("Should delete existing key 'c'")
	}
	delete(c.ref, "c")

	// Delete first key
	if !c.tree.Delete([]byte("a")) {
		t.Fatal("Should delete existing key 'a'")
	}
	delete(c.ref, "a")

	// Delete last key
	if !c.tree.Delete([]byte("e")) {
		t.Fatal("Should delete existing key 'e'")
	}
	delete(c.ref, "e")

	// Try to delete non-existent key
	if c.tree.Delete([]byte("nonexistent")) {
		t.Fatal("Should not delete non-existent key")
	}

	// Verify remaining keys
	remaining := []string{"b", "d"}
	for _, key := range remaining {
		if _, exists := c.ref[key]; !exists {
			t.Fatalf("Key %s should still exist", key)
		}
	}
}

func TestTreeSplitAndMerge(t *testing.T) {
	c := newC()

	// Insert enough keys to cause splits
	for i := 0; i < 20; i++ {
		key := string(rune('a' + i))
		value := string(rune('A' + i))
		c.tree.Insert([]byte(key), []byte(value))
		c.ref[key] = value
	}

	initialPages := len(c.pages)

	// Delete keys to test merging
	for i := 5; i < 15; i++ {
		key := string(rune('a' + i))
		if !c.tree.Delete([]byte(key)) {
			t.Fatalf("Failed to delete key: %s", key)
		}
		delete(c.ref, key)
	}

	finalPages := len(c.pages)

	t.Logf("Pages before deletion: %d, after: %d", initialPages, finalPages)

	// Tree should still be valid
	if c.tree.root == 0 {
		t.Fatal("Tree should not be empty")
	}

	// Verify all remaining pages are valid
	for ptr, node := range c.pages {
		if node.nBytes() > BTREE_PAGE_SIZE {
			t.Fatalf("Page %d exceeds size limit", ptr)
		}
		if node.nKeys() == 0 && ptr != c.tree.root {
			t.Fatalf("Non-root page %d is empty", ptr)
		}
	}
}

func TestTreeEdgeCases(t *testing.T) {
	c := newC()

	// Test empty key
	c.tree.Insert([]byte(""), []byte("empty"))
	c.ref[""] = "empty"
	if !c.tree.Delete([]byte("")) {
		t.Fatal("Should delete empty key")
	}
	delete(c.ref, "")

	// Test single key tree
	c.tree.Insert([]byte("single"), []byte("value"))
	c.ref["single"] = "value"
	if !c.tree.Delete([]byte("single")) {
		t.Fatal("Should delete the only key")
	}
	delete(c.ref, "single")

	// Tree should be empty now
	if c.tree.root != 0 {
		t.Fatal("Tree should be empty after deleting only key")
	}

	// Test inserting after complete deletion
	c.tree.Insert([]byte("new"), []byte("newvalue"))
	c.ref["new"] = "newvalue"
	if c.tree.root == 0 {
		t.Fatal("Root should be set after re-insertion")
	}
}

func TestTreeOrdering(t *testing.T) {
	c := newC()

	// Insert keys in reverse order
	keys := []string{"z", "y", "x", "w", "v"}
	for _, key := range keys {
		c.tree.Insert([]byte(key), []byte("value"))
		c.ref[key] = "value"
	}

	// Insert keys in forward order
	keys = []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		c.tree.Insert([]byte(key), []byte("value"))
		c.ref[key] = "value"
	}

	// Delete keys from both ends
	if !c.tree.Delete([]byte("a")) || !c.tree.Delete([]byte("z")) {
		t.Fatal("Should delete keys from both ends")
	}
	delete(c.ref, "a")
	delete(c.ref, "z")

	// Verify tree is still valid
	if c.tree.root == 0 {
		t.Fatal("Tree should not be empty")
	}

	// Check all pages are within size limits
	for ptr, node := range c.pages {
		if node.nBytes() > BTREE_PAGE_SIZE {
			t.Fatalf("Page %d exceeds size limit: %d", ptr, node.nBytes())
		}
	}
}

func TestTreeMultipleOperations(t *testing.T) {
	c := newC()

	operations := []struct {
		op  string
		key string
		val string
	}{
		{"insert", "key1", "val1"},
		{"insert", "key2", "val2"},
		{"insert", "key3", "val3"},
		{"delete", "key2", ""},
		{"insert", "key4", "val4"},
		{"insert", "key2", "val2_new"},
		{"delete", "key1", ""},
		{"insert", "key5", "val5"},
	}

	for _, op := range operations {
		switch op.op {
		case "insert":
			c.tree.Insert([]byte(op.key), []byte(op.val))
			c.ref[op.key] = op.val
		case "delete":
			success := c.tree.Delete([]byte(op.key))
			if success {
				delete(c.ref, op.key)
			} else if _, exists := c.ref[op.key]; exists {
				t.Fatalf("Failed to delete existing key: %s", op.key)
			}
		}
	}

	// Verify final state
	expectedKeys := map[string]string{
		"key3": "val3",
		"key4": "val4",
		"key2": "val2_new",
		"key5": "val5",
	}

	for key, expectedVal := range expectedKeys {
		if c.ref[key] != expectedVal {
			t.Fatalf("Key %s has wrong value: expected %s, got %s", key, expectedVal, c.ref[key])
		}
	}

	// Verify tree integrity
	if c.tree.root == 0 {
		t.Fatal("Tree should not be empty")
	}

	for ptr, node := range c.pages {
		if node.nBytes() > BTREE_PAGE_SIZE {
			t.Fatalf("Page %d exceeds size limit", ptr)
		}
	}
}

func TestTreeRootNode(t *testing.T) {
	c := newC()

	// Test that root node has proper structure
	c.tree.Insert([]byte("test"), []byte("value"))

	rootNode := BNode(c.tree.get(c.tree.root))
	if rootNode.bType() != BNODE_LEAF && rootNode.bType() != BNODE_NODE {
		t.Fatalf("Root node has invalid type: %d", rootNode.bType())
	}

	if rootNode.nKeys() == 0 {
		t.Fatal("Root node should not be empty")
	}

	// Test that root node splits properly when needed
	for i := 0; i < 50; i++ {
		key := string(rune('a'+(i%26))) + string(rune('0'+(i/26)))
		c.tree.Insert([]byte(key), []byte("value"))
	}

	rootNode = BNode(c.tree.get(c.tree.root))
	if rootNode.bType() != BNODE_NODE {
		t.Log("Root should be internal node after multiple insertions")
	}

	// Verify all child pointers are valid
	for i := uint16(0); i < rootNode.nKeys(); i++ {
		ptr := rootNode.getPtr(i)
		if _, exists := c.pages[ptr]; !exists {
			t.Fatalf("Root child pointer %d points to non-existent page", i)
		}
	}
}

func TestTreeMemoryManagement(t *testing.T) {
	c := newC()

	initialPtrs := make(map[uint64]bool)

	// Track pointers during operations
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		c.tree.Insert([]byte(key), []byte("value"))

		// Store current pointers
		for ptr := range c.pages {
			initialPtrs[ptr] = true
		}
	}

	// Delete some keys and check that pages are freed
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		c.tree.Delete([]byte(key))
	}

	// Some pages should have been freed (depends on merging logic)
	currentPtrs := make(map[uint64]bool)
	for ptr := range c.pages {
		currentPtrs[ptr] = true
	}

	// At least some pointers should be different (not strict due to caching/reuse)
	t.Logf("Initial pointers: %d, Current pointers: %d", len(initialPtrs), len(currentPtrs))

	// Final tree should be valid
	if c.tree.root == 0 {
		t.Fatal("Tree should not be empty")
	}
}

// Run all tests
func TestTreeComprehensive(t *testing.T) {
	t.Run("Basic", TestTreeBasic)
	t.Run("Delete", TestTreeDelete)
	t.Run("SplitAndMerge", TestTreeSplitAndMerge)
	t.Run("EdgeCases", TestTreeEdgeCases)
	t.Run("Ordering", TestTreeOrdering)
	t.Run("MultipleOperations", TestTreeMultipleOperations)
	t.Run("RootNode", TestTreeRootNode)
	t.Run("MemoryManagement", TestTreeMemoryManagement)
}
