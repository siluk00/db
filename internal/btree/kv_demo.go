package btree

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

// Main demonstration
func RunDemo() {
	fmt.Println("=== Append-Only KV Store Demonstration ===")

	// Clean up old database
	os.Remove("test.db")

	// Create and open database
	db := &KV{Path: "test.db"}

	// Open file
	fd, err := createFileSync(db.Path)
	if err != nil {
		panic(err)
	}
	db.fd = fd
	db.Open()

	// Initialize empty database
	db.page.flushed = 1
	db.tree.root = 0

	fmt.Println("1️⃣  Starting with empty database")
	db.DumpState()

	// Insert first key
	fmt.Println("2️⃣  Inserting key1=value1")
	db.Set([]byte("key1"), []byte("value1"))
	db.DumpState()

	// Insert second key
	fmt.Println("3️⃣  Inserting key2=value2")
	db.Set([]byte("key2"), []byte("value2"))
	db.DumpState()

	// Read values
	fmt.Println("4️⃣  Reading values")
	val, ok := db.Get([]byte("key1"))
	fmt.Printf("   key1: %s (found: %v)\n", val, ok)
	val, ok = db.Get([]byte("key2"))
	fmt.Printf("   key2: %s (found: %v)\n", val, ok)
	val, ok = db.Get([]byte("key3"))
	fmt.Printf("   key3: %s (found: %v)\n", val, ok)

	// Delete a key
	fmt.Println("\n5️⃣  Deleting key1")
	db.Del([]byte("key1"))
	db.DumpState()

	// Verify deletion
	fmt.Println("6️⃣  Verifying deletion")
	val, ok = db.Get([]byte("key1"))
	fmt.Printf("   key1: %s (found: %v)\n", val, ok)

	// Close and reopen to test persistence
	fmt.Println("\n7️⃣  Closing and reopening database")
	db.Close()

	// Reopen
	db2 := &KV{Path: "test.db"}
	fd2, err := syscall.Open(db2.Path, syscall.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	db2.fd = fd2
	db2.Open()

	// Get file size
	stat := &syscall.Stat_t{}
	syscall.Fstat(fd2, stat)

	// Map the file
	extendMap(db2, int(stat.Size))

	// Read metadata
	if stat.Size > 0 {
		db2.mmap.chunks[0] = db2.mmap.chunks[0][:stat.Size]
		data := db2.mmap.chunks[0]
		loadMeta(db2, data)
	}

	fmt.Println("\n8️⃣  After reopening:")
	db2.DumpState()

	// Try to read keys
	fmt.Println("9️⃣  Reading from reopened database")
	val, ok = db2.Get([]byte("key2"))
	fmt.Printf("   key2: %s (found: %v)\n", val, ok)

	// Insert one more
	fmt.Println("\n🔟  Inserting key3=value3")
	db2.Set([]byte("key3"), []byte("value3"))
	db2.DumpState()

	// Final read
	fmt.Println("Final values:")
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for _, key := range keys {
		val, ok := db2.Get(key)
		if ok {
			fmt.Printf("  %s = %s\n", key, val)
		} else {
			fmt.Printf("  %s = (not found)\n", key)
		}
	}

	// Clean up
	db2.Close()
	fmt.Println("\n✅ Demonstration complete. Check test.db file")
}

func (db *KV) DumpState() {
	fmt.Println("\n=== KV Store State ===")
	fmt.Printf("File: %s\n", db.Path)
	fmt.Printf("fd: %d\n", db.fd)
	fmt.Printf("Root pointer: %d\n", db.tree.root)
	fmt.Printf("Flushed pages: %d\n", db.page.flushed)
	fmt.Printf("Temp pages: %d\n", len(db.page.temp))
	fmt.Printf("Mmap total: %d bytes\n", db.mmap.total)
	fmt.Printf("Mmap chunks: %d\n", len(db.mmap.chunks))

	// Read and display metadata from disk
	if db.mmap.total > 0 {
		meta := db.mmap.chunks[0][:32]
		sig := string(meta[:16])
		root := binary.LittleEndian.Uint64(meta[16:24])
		flushed := binary.LittleEndian.Uint64(meta[24:32])
		fmt.Printf("\nDisk Metadata:\n")
		fmt.Printf("  Signature: %s\n", sig)
		fmt.Printf("  Root: %d\n", root)
		fmt.Printf("  Flushed: %d\n", flushed)
	}
	fmt.Println("=====================")
}

func (db *KV) Close() error {
	// Unmap all chunks
	for _, chunk := range db.mmap.chunks {
		syscall.Munmap(chunk)
	}
	// Close file
	return syscall.Close(db.fd)
}
