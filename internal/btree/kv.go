package btree

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"syscall"

	"golang.org/x/sys/unix"
)

const DB_SIG = "DB6"

type KV struct {
	Path string //file name
	fd   int    // file descriptor
	tree BTree
	mmap struct {
		total  int      //mmap size
		chunks [][]byte //multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64   //number of pages permanently written on disk
		temp    [][]byte //newly allocated pages
	}
	failed bool
}

// initialize KV store struct
func (db *KV) Open() error {
	db.tree.get = db.pageRead
	db.tree.newBNode = db.pageAppend
	db.tree.del = func(uint64) {}
	return nil
}

// wrapper funtion  for getting value for ky, returns true if key exists
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

// wrapper function to Insert key and value on Btree
// synchronizes everything
func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db)
	db.tree.Insert(key, val)
	return updateOrRevert(db, meta)
}

// Write temp to disc, synchronizes, write meta to db root and synchronizes again
// if error -> sets db.failed to true and saves the snapshot to before the error
func updateOrRevert(db *KV, meta []byte) error {
	if db.failed {
		db.failed = false
	}

	err := updateFile(db)

	if err != nil {
		db.failed = true
		loadMeta(db, meta)
		db.page.temp = db.page.temp[:0]
	}

	return err
}

// deletes key and value for given key, returns true if value exists
func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, updateFile(db)
}

// Write all temp to disc, synchronizes, write meta to db and synchronizes again
func updateFile(db *KV) error {
	// write all temp files to disc
	if err := writePages(db); err != nil {
		return err
	}

	// Force ordering
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}

	// update root pointer atomically
	if err := updateRoot(db); err != nil {
		return err
	}

	// make everything persistent
	return syscall.Fsync(db.fd)
}

// Write all temp files to disc
func writePages(db *KV) error {
	//extending the map if needed
	size := ((int)(db.page.flushed) + len(db.page.temp)) * BTREE_PAGE_SIZE
	if err := extendMap(db, size); err != nil {
		return err
	}

	//write the pages to file
	offset := int64(db.page.flushed * BTREE_PAGE_SIZE)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}

	//discard memory data
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]

	return nil
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 {
		db.page.flushed = 1 //the meta page is initialized on the first page
		return nil
	}

	//read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	//verify the page
	//TODO

	return nil
}

// rewrites Meta to root
func updateRoot(db *KV) error {
	// Pwrite is used here so several threads can write to file at the same time without need to block
	// It means positional write, the offset is completely stateless
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}

	return nil
}

func createFileSync(file string) (int, error) {
	// getting syscall open for safety against directory renaming, and to use it in the next suyscalls
	// and also to ensure the file string passed is really a directory
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)

	// OpenAt protects against simultenous renaming of files
	// also protects against changing to symlink
	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}

	// creating a file is basically writing on parent directory
	if err = syscall.Fsync(dirfd); err != nil {
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}

	return fd, nil
}

func (db *KV) pageRead(ptr uint64) []byte { // get method from BTree
	start := uint64(0)

	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE // end-start = amount of pages
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)     // size of pages times the amount of page, calculate the offset where the page is
			return chunk[offset : offset+BTREE_PAGE_SIZE] // returns the page in the pointer ptr
		}
		start = end
	}

	panic("bad pointer")
}

// database + current size of database
func extendMap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil
	}

	alloc := max(db.mmap.total, 64<<20) //double the current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}

	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// this is the tree.NewBNode function for KV store
// Appends the node to db.page.temp
// Returns the index of the new appended node
func (db *KV) pageAppend(node []byte) (uint64, error) {
	ptr := db.page.flushed + uint64(len(db.page.temp)) // amount of pages on Btree already written to disc + amount of temp pages
	db.page.temp = append(db.page.temp, node)          // append to temp
	return ptr, nil
}

// backup snapshot of operation , gets the meta from db.tree
func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

// provides snapshot isolation writing the pointer to root and the amount of nodes already written in db.tree
func loadMeta(db *KV, data []byte) {
	sig := string(data[:16])
	if sig != DB_SIG {
		panic("invalid database signature")
	}

	db.tree.root = binary.LittleEndian.Uint64(data[16:24])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:32])
}
